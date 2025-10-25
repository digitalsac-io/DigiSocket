import NodeCache from '@cacheable/node-cache'
import { Boom } from '@hapi/boom'
import { proto } from '../../WAProto/index.js'
import ListType = proto.Message.ListMessage.ListType;
import { DEFAULT_CACHE_TTLS, WA_DEFAULT_EPHEMERAL } from '../Defaults'
import type {
	AnyMessageContent,
	MediaConnInfo,
	MessageReceiptType,
	MessageRelayOptions,
	MiscMessageGenerationOptions,
	SocketConfig,
	WAMessage,
	WAMessageKey
} from '../Types'
import {
	aggregateMessageKeysNotFromMe,
	assertMediaContent,
	bindWaitForEvent,
	decryptMediaRetryData,
	encodeNewsletterMessage,
	encodeSignedDeviceIdentity,
	encodeWAMessage,
	encryptMediaRetryRequest,
	extractDeviceJids,
	generateMessageIDV2,
	generateParticipantHashV2,
	generateWAMessage,
	getStatusCodeForMediaRetry,
	getUrlFromDirectPath,
	getWAUploadToServer,
	MessageRetryManager,
	normalizeMessageContent,
	parseAndInjectE2ESessions,
	unixTimestampSeconds
} from '../Utils'
import { getUrlInfo } from '../Utils/link-preview'
import { makeKeyedMutex } from '../Utils/make-mutex'
import {
	areJidsSameUser,
	type BinaryNode,
	type BinaryNodeAttributes,
	type FullJid,
	getBinaryNodeChild,
	getBinaryNodeChildren,
	getServerFromDomainType,
	isJidGroup,
	isHostedLidUser,
	isHostedPnUser,
	isLidUser,
	isPnUser,
	jidDecode,
	jidEncode,
	jidNormalizedUser,
	type JidWithDevice,
	S_WHATSAPP_NET
} from '../WABinary'
import { USyncQuery, USyncUser } from '../WAUSync'
import { makeNewsletterSocket } from './newsletter'

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

function normalizeGroupJidsInContent(jid: string, content: AnyMessageContent, options?: any) {
	if (options && 'participant' in options) {
		delete options.participant
	}
	
	if (content && typeof content === 'object' && 'mentions' in content && Array.isArray(content.mentions)) {
		content.mentions = content.mentions.map((m: string) => {
			try {
				return jidNormalizedUser(m)
			} catch {
				return m
			}
		})
	}
	
	if (options?.quoted?.key?.participant) {
		try {
			options.quoted.key.participant = jidNormalizedUser(options.quoted.key.participant)
		} catch {
		}
	}
}

async function progressiveAssertSessions(
	assertSessionsFn: (jids: string[]) => Promise<any>,
	allJids: string[],
	chunk: number,
	delayMs: number,
	logger: any
) {
	if (allJids.length === 0) return
	
	logger.info({ total: allJids.length, chunk, delayMs }, '🔄 Iniciando assertSessions progressivo em lotes')
	
	for (let i = 0; i < allJids.length; i += chunk) {
		const batch = allJids.slice(i, i + chunk)
		logger.debug({ batchSize: batch.length, progress: `${i + batch.length}/${allJids.length}` }, '📦 Processando lote')
		
		try {
			await assertSessionsFn(batch)
		} catch (err) {
			logger.warn({ batchSize: batch.length, error: String(err) }, '⚠️ Erro em lote ignorado')
		}
		
		if (i + chunk < allJids.length) {
			await new Promise(resolve => setTimeout(resolve, delayMs))
		}
	}
	
	logger.info({ total: allJids.length }, '✅ assertSessions progressivo concluído')
}

export const makeMessagesSocket = (config: SocketConfig) => {
	const {
		logger,
		linkPreviewImageThumbnailWidth,
		generateHighQualityLinkPreview,
		options: axiosOptions,
		patchMessageBeforeSending,
		cachedGroupMetadata,
		enableRecentMessageCache,
		maxMsgRetryCount,
		transformAudio,
		compatV6GroupSend = true,
		groupAssertChunk = 10,
		groupAssertDelayMs = 250,
		recentMessagesCacheSize = 20000
	} = config
	const sock = makeNewsletterSocket(config)
	const {
		ev,
		authState,
		processingMutex,
		signalRepository,
		upsertMessage,
		query,
		fetchPrivacySettings,
		sendNode,
		groupMetadata,
		groupToggleEphemeral
	} = sock

	const userDevicesCache =
		config.userDevicesCache ||
		new NodeCache<JidWithDevice[]>({
			stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES, // 5 minutes
			useClones: false
		})

	const peerSessionsCache = new NodeCache<boolean>({
		stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES,
		useClones: false
	})
	
	// LID Cache from v6 - stores LID mappings for 1 hour
	const lidCache = new NodeCache<string>({
		stdTTL: 3600, // 1 hour
		useClones: false
	})

	// Initialize message retry manager if enabled
	const messageRetryManager = enableRecentMessageCache 
		? new MessageRetryManager(logger, maxMsgRetryCount, recentMessagesCacheSize) 
		: null

	// Prevent race conditions in Signal session encryption by user
	const encryptionMutex = makeKeyedMutex()

	let mediaConn: Promise<MediaConnInfo>
	const refreshMediaConn = async (forceGet = false) => {
		const media = await mediaConn
		if (!media || forceGet || new Date().getTime() - media.fetchDate.getTime() > media.ttl * 1000) {
			mediaConn = (async () => {
				const result = await query({
					tag: 'iq',
					attrs: {
						type: 'set',
						xmlns: 'w:m',
						to: S_WHATSAPP_NET
					},
					content: [{ tag: 'media_conn', attrs: {} }]
				})
				const mediaConnNode = getBinaryNodeChild(result, 'media_conn')!
				// TODO: explore full length of data that whatsapp provides
				const node: MediaConnInfo = {
					hosts: getBinaryNodeChildren(mediaConnNode, 'host').map(({ attrs }) => ({
						hostname: attrs.hostname!,
						maxContentLengthBytes: +attrs.maxContentLengthBytes!
					})),
					auth: mediaConnNode.attrs.auth!,
					ttl: +mediaConnNode.attrs.ttl!,
					fetchDate: new Date()
				}
				logger.debug('fetched media conn')
				return node
			})()
		}

		return mediaConn
	}

	/**
	 * generic send receipt function
	 * used for receipts of phone call, read, delivery etc.
	 * */
	const sendReceipt = async (
		jid: string,
		participant: string | undefined,
		messageIds: string[],
		type: MessageReceiptType
	) => {
		if (!messageIds || messageIds.length === 0) {
			throw new Boom('missing ids in receipt')
		}

		const node: BinaryNode = {
			tag: 'receipt',
			attrs: {
				id: messageIds[0]!
			}
		}
		const isReadReceipt = type === 'read' || type === 'read-self'
		if (isReadReceipt) {
			node.attrs.t = unixTimestampSeconds().toString()
		}

		if (type === 'sender' && (isPnUser(jid) || isLidUser(jid))) {
			node.attrs.recipient = jid
			node.attrs.to = participant!
		} else {
			node.attrs.to = jid
			if (participant) {
				node.attrs.participant = participant
			}
		}

		if (type) {
			node.attrs.type = type
		}

		const remainingMessageIds = messageIds.slice(1)
		if (remainingMessageIds.length) {
			node.content = [
				{
					tag: 'list',
					attrs: {},
					content: remainingMessageIds.map(id => ({
						tag: 'item',
						attrs: { id }
					}))
				}
			]
		}

		logger.debug({ attrs: node.attrs, messageIds }, 'sending receipt for messages')
		await sendNode(node)
	}

	/** Correctly bulk send receipts to multiple chats, participants */
	const sendReceipts = async (keys: WAMessageKey[], type: MessageReceiptType) => {
		const recps = aggregateMessageKeysNotFromMe(keys)
		for (const { jid, participant, messageIds } of recps) {
			await sendReceipt(jid, participant, messageIds, type)
		}
	}

	/** Bulk read messages. Keys can be from different chats & participants */
	const readMessages = async (keys: WAMessageKey[]) => {
		const privacySettings = await fetchPrivacySettings()
		// based on privacy settings, we have to change the read type
		const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self'
		await sendReceipts(keys, readType)
	}

	/** Device info with wire JID */
	type DeviceWithJid = JidWithDevice & {
		jid: string
	}

	/** Fetch all the devices we've to send a message to */
	const getUSyncDevices = async (
		jids: string[],
		useCache: boolean,
		ignoreZeroDevices: boolean
	): Promise<DeviceWithJid[]> => {
		const deviceResults: DeviceWithJid[] = []

		if (!useCache) {
			logger.debug('not using cache for devices')
		}

		const toFetch: string[] = []

		const jidsWithUser = jids
			.map(jid => {
				const decoded = jidDecode(jid)
				const user = decoded?.user
				const device = decoded?.device
				const isExplicitDevice = typeof device === 'number' && device >= 0

			if (isExplicitDevice && user) {
				deviceResults.push({
					user,
					device,
					jid
				})
				return null
			}

			if (!isLidUser(jid)) {
				jid = jidNormalizedUser(jid)
			}
			return { jid, user }
			})
			.filter(jid => jid !== null)

		let mgetDevices: undefined | Record<string, DeviceWithJid[] | undefined>

		if (useCache && userDevicesCache.mget) {
			const usersToFetch = jidsWithUser.map(j => j?.user).filter(Boolean) as string[]
			mgetDevices = await userDevicesCache.mget(usersToFetch) as any
		}

		for (const { jid, user } of jidsWithUser) {
			if (useCache) {
				const devices =
					mgetDevices?.[user!] ||
					(userDevicesCache.mget ? undefined : ((await userDevicesCache.get(user!)) as DeviceWithJid[]))
				if (devices) {
					deviceResults.push(...devices)
					logger.debug({ user, deviceCount: devices.length, hasJid: !!(devices[0] as any)?.jid }, 'using cache for devices')
				} else {
					toFetch.push(jid)
				}
			} else {
				toFetch.push(jid)
			}
		}

		if (!toFetch.length) {
			return deviceResults
		}

		const query = new USyncQuery().withContext('message').withDeviceProtocol().withLIDProtocol()

		for (const jid of toFetch) {
			query.withUser(new USyncUser().withId(jid)) // todo: investigate - the idea here is that <user> should have an inline lid field with the lid being the pn equivalent
		}

		const result = await sock.executeUSyncQuery(query)

		if (result) {
			// TODO: LID MAP this stuff (lid protocol will now return lid with devices)
			const lidResults = result.list.filter(a => !!a.lid)
			if (lidResults.length > 0) {
				logger.trace('Storing LID maps from device call')
				await signalRepository.lidMapping.storeLIDPNMappings(lidResults.map(a => ({ lid: a.lid as string, pn: a.id })))
			}

			const extracted = extractDeviceJids(
				result?.list,
				authState.creds.me!.id,
				authState.creds.me!.lid!,
				ignoreZeroDevices
			)
			const deviceMap: { [_: string]: DeviceWithJid[] } = {}

			for (const item of extracted) {
				const finalJid = (item as any).originalJid 
					? jidEncode((item as any).originalJid.split('@')[0].split(':')[0], 
					            (item as any).originalJid.split('@')[1], 
					            item.device)
					: jidEncode(item.user, item.server, item.device)

				const itemWithJid: DeviceWithJid = {
					...item,
					jid: finalJid
				}

				deviceMap[item.user] = deviceMap[item.user] || []
				deviceMap[item.user]?.push(itemWithJid)
				
				deviceResults.push(itemWithJid)
			}

			if (userDevicesCache.mset) {
				// if the cache supports mset, we can set all devices in one go
				await userDevicesCache.mset(Object.entries(deviceMap).map(([key, value]) => ({ key, value })))
				logger.debug({ userCount: Object.keys(deviceMap).length, withJid: true }, '💾 Salvando devices no cache com JID')
			} else {
				for (const key in deviceMap) {
					if (deviceMap[key]) await userDevicesCache.set(key, deviceMap[key])
				}
				logger.debug({ userCount: Object.keys(deviceMap).length, withJid: true }, '💾 Salvando devices no cache com JID')
			}

			const userDeviceUpdates: { [userId: string]: string[] } = {}
			for (const [userId, devices] of Object.entries(deviceMap)) {
				if (devices && devices.length > 0) {
					userDeviceUpdates[userId] = devices.map(d => d.device?.toString() || '0')
				}
			}

			if (Object.keys(userDeviceUpdates).length > 0) {
				try {
					await authState.keys.set({ 'device-list': userDeviceUpdates })
					logger.debug(
						{ userCount: Object.keys(userDeviceUpdates).length },
						'stored user device lists for bulk migration'
					)
				} catch (error) {
					logger.warn({ error }, 'failed to store user device lists')
				}
			}
		}

		return deviceResults
	}

	const assertSessions = async (jids: string[]) => {
		if (compatV6GroupSend) {
			try {
				return await assertSessionsInternal(jids)
			} catch (err) {
				logger.debug({ jidsCount: jids.length, error: String(err) }, '🔄 Modo v6: Erro no assertSessions ignorado (sessões criadas sob demanda)')
				return false
			}
		}
		
		// Fluxo normal v7
		return await assertSessionsInternal(jids)
	}
	
	const assertSessionsInternal = async (jids: string[]) => {
		let didFetchNewSession = false
		const uniqueJids = [...new Set(jids)] // Deduplicate JIDs
		const jidsRequiringFetch: string[] = []

		logger.debug({ jids }, 'assertSessions call with jids')

		// Check peerSessionsCache and validate sessions using libsignal loadSession
		for (const jid of uniqueJids) {
			const signalId = signalRepository.jidToSignalProtocolAddress(jid)
			const cachedSession = peerSessionsCache.get(signalId)
			if (cachedSession !== undefined) {
				if (cachedSession) {
					continue // Session exists in cache
				}
			} else {
				const sessionValidation = await signalRepository.validateSession(jid)
				const hasSession = sessionValidation.exists
				peerSessionsCache.set(signalId, hasSession)
				if (hasSession) {
					continue
				}
			}

			jidsRequiringFetch.push(jid)
		}

		if (jidsRequiringFetch.length) {
			// LID if mapped, otherwise original
			const wireJids = [
				...jidsRequiringFetch.filter(jid => !!isLidUser(jid) || !!isHostedLidUser(jid)),
				...(
					(await signalRepository.lidMapping.getLIDsForPNs(
						jidsRequiringFetch.filter(jid => !!isPnUser(jid) || !!isHostedPnUser(jid))
					)) || []
				).map(a => a.lid)
			]

			logger.debug({ jidsRequiringFetch, wireJids }, 'fetching sessions')
			const result = await query({
				tag: 'iq',
				attrs: {
					xmlns: 'encrypt',
					type: 'get',
					to: S_WHATSAPP_NET
				},
				content: [
					{
						tag: 'key',
						attrs: {},
						content: wireJids.map(jid => ({
							tag: 'user',
							attrs: { jid }
						}))
					}
				]
			})
			await parseAndInjectE2ESessions(result, signalRepository)
			didFetchNewSession = true

			// Cache fetched sessions using wire JIDs
			for (const wireJid of wireJids) {
				const signalId = signalRepository.jidToSignalProtocolAddress(wireJid)
				peerSessionsCache.set(signalId, true)
			}
		}

		return didFetchNewSession
	}

	const sendPeerDataOperationMessage = async (
		pdoMessage: proto.Message.IPeerDataOperationRequestMessage
	): Promise<string> => {
		//TODO: for later, abstract the logic to send a Peer Message instead of just PDO - useful for App State Key Resync with phone
		if (!authState.creds.me?.id) {
			throw new Boom('Not authenticated')
		}

		const protocolMessage: proto.IMessage = {
			protocolMessage: {
				peerDataOperationRequestMessage: pdoMessage,
				type: proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE
			}
		}

		const meJid = jidNormalizedUser(authState.creds.me.id)

		const msgId = await relayMessage(meJid, protocolMessage, {
			additionalAttributes: {
				category: 'peer',

				push_priority: 'high_force'
			},
			additionalNodes: [
				{
					tag: 'meta',
					attrs: { appdata: 'default' }
				}
			]
		})

		return msgId
	}

	const createParticipantNodes = async (
		recipientJids: string[],
		message: proto.IMessage,
		extraAttrs?: BinaryNode['attrs'],
		dsmMessage?: proto.IMessage
	) => {
		if (!recipientJids.length) {
			return { nodes: [] as BinaryNode[], shouldIncludeDeviceIdentity: false }
		}

		const patched = await patchMessageBeforeSending(message, recipientJids)
		const patchedMessages = Array.isArray(patched)
			? patched
			: recipientJids.map(jid => ({ recipientJid: jid, message: patched }))

		let shouldIncludeDeviceIdentity = false
		const meId = authState.creds.me!.id
		const meLid = authState.creds.me?.lid
		const meLidUser = meLid ? jidDecode(meLid)?.user : null
		const useV6Compat = compatV6GroupSend

		const encryptionPromises = (patchedMessages as any).map(
			async ({ recipientJid: jid, message: patchedMessage }: any) => {
				if (!jid) return null
				let msgToEncrypt = patchedMessage
				if (dsmMessage) {
					const { user: targetUser } = jidDecode(jid)!
					const { user: ownPnUser } = jidDecode(meId)!
					const ownLidUser = meLidUser
					const isOwnUser = targetUser === ownPnUser || (ownLidUser && targetUser === ownLidUser)
					const isExactSenderDevice = jid === meId || (meLid && jid === meLid)
					if (isOwnUser && !isExactSenderDevice) {
						msgToEncrypt = dsmMessage
						logger.debug({ jid, targetUser }, 'Using DSM for own device')
					}
				}

				const bytes = encodeWAMessage(msgToEncrypt)
				
				try {
					let encResult
					if (useV6Compat) {
						encResult = await signalRepository.encryptMessage({
							jid,
							data: bytes
						})
					} else {
						const mutexKey = jid
						encResult = await encryptionMutex.mutex(mutexKey, async () => {
							return await signalRepository.encryptMessage({
								jid,
								data: bytes
							})
						})
					}
					
					const { type, ciphertext } = encResult
					if (type === 'pkmsg') {
						shouldIncludeDeviceIdentity = true
					}

					return {
						tag: 'to',
						attrs: { jid },
						content: [
							{
								tag: 'enc',
								attrs: {
									v: '2',
									type,
									...(extraAttrs || {})
								},
								content: ciphertext
							}
						]
					}
				} catch (err) {
					if (useV6Compat) {
						logger.warn({ jid, error: String(err) }, '🔄 Modo v6: Erro ao encriptar para device ignorado')
						return null
					}
					throw err
				}
			}
		)

		const nodes = (await Promise.all(encryptionPromises)).filter(node => node !== null) as BinaryNode[]
		return { nodes, shouldIncludeDeviceIdentity }
	}

	const relayMessage = async (
		jid: string,
		message: proto.IMessage,
		{
			messageId: msgId,
			participant,
			additionalAttributes,
			additionalNodes,
			useUserDevicesCache,
			useCachedGroupMetadata,
			statusJidList
		}: MessageRelayOptions
	) => {
		const meId = authState.creds.me!.id
		const meLid = authState.creds.me?.lid
		const meLidUser = meLid ? jidDecode(meLid)?.user : null
		const isRetryResend = Boolean(participant?.jid)
		let shouldIncludeDeviceIdentity = isRetryResend
		const statusJid = 'status@broadcast'

		const { user, server } = jidDecode(jid)!
		const isGroup = server === 'g.us'
		const isStatus = jid === statusJid
		const isLid = server === 'lid'
		const isNewsletter = server === 'newsletter'
		const finalJid = jid

		msgId = msgId || generateMessageIDV2(meId)
		useUserDevicesCache = useUserDevicesCache !== false
		useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus

		const participants: BinaryNode[] = []
		const destinationJid = !isStatus ? finalJid : statusJid
		const binaryNodeContent: BinaryNode[] = []
		const devices: DeviceWithJid[] = []

		const meMsg: proto.IMessage = {
			deviceSentMessage: {
				destinationJid,
				message
			},
			messageContextInfo: message.messageContextInfo
		}

		const extraAttrs: BinaryNodeAttributes = {}

		if (participant) {
			if (!isGroup && !isStatus) {
				additionalAttributes = { ...additionalAttributes, device_fanout: 'false' }
			}

			const { user, device } = jidDecode(participant.jid)!
			devices.push({
				user,
				device,
				jid: participant.jid
			})
		}

		await authState.keys.transaction(async () => {
			const mediaType = getMediaType(message)
			if (mediaType) {
				extraAttrs['mediatype'] = mediaType
			}

			if (isNewsletter) {
				const patched = patchMessageBeforeSending ? await patchMessageBeforeSending(message, []) : message
				const bytes = encodeNewsletterMessage(patched as proto.IMessage)
				binaryNodeContent.push({
					tag: 'plaintext',
					attrs: {},
					content: bytes
				})
				const stanza: BinaryNode = {
					tag: 'message',
					attrs: {
						to: jid,
						id: msgId,
						type: getMessageType(message),
						...(additionalAttributes || {})
					},
					content: binaryNodeContent
				}
				logger.debug({ msgId }, `sending newsletter message to ${jid}`)
				await sendNode(stanza)
				return
			}

			if (normalizeMessageContent(message)?.pinInChatMessage) {
				extraAttrs['decrypt-fail'] = 'hide' // todo: expand for reactions and other types
			}

			if (isGroup || isStatus) {
				const [groupData, senderKeyMap] = await Promise.all([
					(async () => {
						// Try user-provided cache first
						let groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined
						
						// Validate cache freshness (max 5 minutes)
						const CACHE_MAX_AGE = 5 * 60 * 1000
						if (groupData && (groupData as any)._cachedAt) {
							const cacheAge = Date.now() - (groupData as any)._cachedAt
							if (cacheAge > CACHE_MAX_AGE) {
								logger.trace({ jid, cacheAge }, 'cache expired, will fetch fresh metadata')
								groupData = undefined
							}
						}
						
						if (groupData && Array.isArray(groupData?.participants)) {
							logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata')
						} else if (!isStatus) {
							// Fallback: try to get from internal cache (getCachedGroupMetadata)
							const cached = typeof (sock as any).getCachedGroupMetadata === 'function' 
								? (sock as any).getCachedGroupMetadata(jid) 
								: undefined
							
							if (cached && cached.participants && cached.participants.length > 0) {
								// Use cached participants list without fetching from WA
								const cacheAge = Date.now() - (cached.updatedAt || 0)
								if (cacheAge < CACHE_MAX_AGE) {
									logger.debug({ 
										jid, 
										participants: cached.participants.length,
										cacheAge 
									}, 'using group metadata from internal cache')
									
									// Build minimal GroupMetadata from cached data
									groupData = {
										id: jid,
										addressingMode: cached.addressingMode,
										participants: cached.participants.map((id: string) => ({ id })),
										subject: '',
										creation: 0,
										owner: undefined
									} as any
								} else {
									logger.trace({ jid, cacheAge }, 'internal cache expired, fetching from WA')
									groupData = await groupMetadata(jid)
								}
							} else {
								// No cache available, fetch from WA
								groupData = await groupMetadata(jid)
							}
						}

						return groupData
					})(),
					(async () => {
						if (!participant && !isStatus) {
							// Get sender-key-memory
							const result = await authState.keys.get('sender-key-memory', [jid])
							const senderKeyMap = result[jid] || {}
							
							// Get LID mappings from internal cache
							if (typeof (sock as any).getLIDMappings === 'function') {
								const mappings = (sock as any).getLIDMappings(jid)
								if (Object.keys(mappings).length > 0) {
									logger.trace({ jid, mappings: Object.keys(mappings).length }, 'loaded LID mappings for sender key')
								}
							}
							
							return senderKeyMap
						}

						return {}
					})()
				])

			if (!participant) {
				let participantsList = (groupData && !isStatus) ? groupData.participants.map(p => p.lid || p.id) : []
				
				if (!isStatus && groupData?.addressingMode === 'lid') {
					const pnParticipants = participantsList.filter(p => isPnUser(p))
					if (pnParticipants.length > 0) {
						logger.debug({ pnCount: pnParticipants.length }, '🔄 Convertendo PNs para LIDs reais')
						const lidMappings = await signalRepository.lidMapping.getLIDsForPNs(pnParticipants)
						if (lidMappings && lidMappings.length > 0) {
							const lidMap = new Map(lidMappings.map(m => [m.pn, m.lid]))
							participantsList = participantsList.map(p => lidMap.get(p) || p)
							logger.debug({ lidCount: lidMappings.length }, '✅ LIDs reais obtidos')
						}
					}
				}
				
				if (isStatus && statusJidList) {
					participantsList.push(...statusJidList)
				}

			if (!isStatus) {
				const addressingMode = groupData?.addressingMode || 'pn'
				additionalAttributes = {
					...additionalAttributes,
					addressing_mode: addressingMode
				}
				logger.debug({ 
					groupJid: destinationJid, 
					addressingMode, 
					fromGroupData: groupData?.addressingMode,
					participantsCount: participantsList.length,
					sampleParticipants: participantsList.slice(0, 3)  // Ver primeiros 3
				}, '📋 Addressing mode e participantes')
			}

				const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false)
				devices.push(...additionalDevices)
				
				const isLidGroup = groupData?.addressingMode === 'lid'
				const myJid = isLidGroup ? meLid : meId
				const myUser = jidDecode(myJid)?.user
				const Mephone = additionalDevices.some(d => d.user === myUser && d.device === 0)
				if (!Mephone && myUser && myJid) {
					devices.push({ user: myUser, device: 0, jid: jidNormalizedUser(myJid) })
					logger.debug({ myJid, isLidGroup }, '✅ Próprio device adicionado')
				}
			}

				if (groupData?.ephemeralDuration && groupData.ephemeralDuration > 0) {
					additionalAttributes = {
						...additionalAttributes,
						expiration: groupData.ephemeralDuration.toString()
					}
				}

				const patched = await patchMessageBeforeSending(message)
				if (Array.isArray(patched)) {
					throw new Boom('Per-jid patching is not supported in groups')
				}

			const bytes = encodeWAMessage(patched)

			const isLidGroup = groupData?.addressingMode === 'lid'
			const groupMeId = isLidGroup && meLid ? meLid : meId
			logger.debug({ 
				groupJid: destinationJid, 
				meId, 
				meLid, 
				isLidGroup,
				usingMeId: groupMeId 
			}, '🔐 Encriptando mensagem de grupo')
			
			const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
				group: destinationJid,
				data: bytes,
				meId: groupMeId
			})

		const senderKeyJids: string[] = []
		
		// V6 compatibility: Add ALL devices to senderKeyJids without pre-validation
		// Let assertSessions handle session establishment and WhatsApp handle retries
		for (const { user, device, jid } of devices) {
			const server = jidDecode(jid)?.server || 'lid'
			const senderId = jidEncode(user, server, device)
			senderKeyJids.push(senderId)
			senderKeyMap[senderId] = true  // v6: mark all devices as having sender key
		}
		
		logger.debug({ senderKeyJidsCount: senderKeyJids.length, devicesCount: devices.length }, 'Preparando envio de sender key para todos devices')

			if (senderKeyJids.length) {
				logger.debug({ senderKeyJids }, 'sending new sender key')

				const senderKeyMsg: proto.IMessage = {
					senderKeyDistributionMessage: {
						axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
						groupId: destinationJid
					}
				}

				await assertSessions(senderKeyJids)

			const result = await createParticipantNodes(senderKeyJids, senderKeyMsg, extraAttrs)
			shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity

			participants.push(...result.nodes)
		}

				if (isRetryResend) {
					const { type, ciphertext: encryptedContent } = await signalRepository.encryptMessage({
						data: bytes,
						jid: participant?.jid!
					})

					binaryNodeContent.push({
						tag: 'enc',
						attrs: {
							v: '2',
							type,
							count: participant!.count.toString()
						},
						content: encryptedContent
					})
				} else {
					binaryNodeContent.push({
						tag: 'enc',
						attrs: { v: '2', type: 'skmsg', ...extraAttrs },
						content: ciphertext
					})

					await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } })
				}
			} else {
				// ADDRESSING CONSISTENCY: Match own identity to conversation context
				// TODO: investigate if this is true
				let ownId = meId
				if (isLid && meLid) {
					ownId = meLid
					logger.debug({ to: jid, ownId }, 'Using LID identity for @lid conversation')
				} else {
					logger.debug({ to: jid, ownId }, 'Using PN identity for @s.whatsapp.net conversation')
				}

				const { user: ownUser } = jidDecode(ownId)!

				if (!participant) {
					const targetUserServer = isLid ? 'lid' : 's.whatsapp.net'
					devices.push({
						user,
						device: 0,
						jid: jidEncode(user, targetUserServer, 0) // rajeh, todo: this entire logic is convoluted and weird.
					})

					if (user !== ownUser) {
						const ownUserServer = isLid ? 'lid' : 's.whatsapp.net'
						const ownUserForAddressing = isLid && meLid ? jidDecode(meLid)!.user : jidDecode(meId)!.user

						devices.push({
							user: ownUserForAddressing,
							device: 0,
							jid: jidEncode(ownUserForAddressing, ownUserServer, 0)
						})
					}

					if (additionalAttributes?.['category'] !== 'peer') {
						// Clear placeholders and enumerate actual devices
						devices.length = 0

						// Use conversation-appropriate sender identity
						const senderIdentity =
							isLid && meLid
								? jidEncode(jidDecode(meLid)?.user!, 'lid', undefined)
								: jidEncode(jidDecode(meId)?.user!, 's.whatsapp.net', undefined)

						// Enumerate devices for sender and target with consistent addressing
						const sessionDevices = await getUSyncDevices([senderIdentity, jid], true, false)
						devices.push(...sessionDevices)

						logger.debug(
							{
								deviceCount: devices.length,
								devices: devices.map(d => `${d.user}:${d.device}@${jidDecode(d.jid)?.server}`)
							},
							'Device enumeration complete with unified addressing'
						)
					}
				}

				const allRecipients: string[] = []
				const meRecipients: string[] = []
				const otherRecipients: string[] = []
				const { user: mePnUser } = jidDecode(meId)!
				const { user: meLidUser } = meLid ? jidDecode(meLid)! : { user: null }

				for (const { user, jid } of devices) {
					const isExactSenderDevice = jid === meId || (meLid && jid === meLid)
					if (isExactSenderDevice) {
						logger.debug({ jid, meId, meLid }, 'Skipping exact sender device (whatsmeow pattern)')
						continue
					}

					// Check if this is our device (could match either PN or LID user)
					const isMe = user === mePnUser || user === meLidUser

					if (isMe) {
						meRecipients.push(jid)
					} else {
						otherRecipients.push(jid)
					}

					allRecipients.push(jid)
				}

				await assertSessions(allRecipients)

				const [
					{ nodes: meNodes, shouldIncludeDeviceIdentity: s1 },
					{ nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }
				] = await Promise.all([
					// For own devices: use DSM if available (1:1 chats only)
					createParticipantNodes(meRecipients, meMsg || message, extraAttrs),
					createParticipantNodes(otherRecipients, message, extraAttrs, meMsg)
				])
				participants.push(...meNodes)
				participants.push(...otherNodes)

				if (meRecipients.length > 0 || otherRecipients.length > 0) {
					extraAttrs['phash'] = generateParticipantHashV2([...meRecipients, ...otherRecipients])
				}

				shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2
			}

			if (participants.length) {
				if (additionalAttributes?.['category'] === 'peer') {
					const peerNode = participants[0]?.content?.[0] as BinaryNode
					if (peerNode) {
						binaryNodeContent.push(peerNode) // push only enc
					}
				} else {
					binaryNodeContent.push({
						tag: 'participants',
						attrs: {},

						content: participants
					})
				}
			}

			const stanza: BinaryNode = {
				tag: 'message',
				attrs: {
					id: msgId,
					to: destinationJid,
					type: getMessageType(message),
					...(additionalAttributes || {})
				},
				content: binaryNodeContent
			}

			// if the participant to send to is explicitly specified (generally retry recp)
			// ensure the message is only sent to that person
			// if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
			if (participant) {
				if (isJidGroup(destinationJid)) {
					stanza.attrs.to = destinationJid
					stanza.attrs.participant = participant.jid
				} else if (areJidsSameUser(participant.jid, meId)) {
					stanza.attrs.to = participant.jid
					stanza.attrs.recipient = destinationJid
				} else {
					stanza.attrs.to = participant.jid
				}
			} else {
				stanza.attrs.to = destinationJid
			}

			if (shouldIncludeDeviceIdentity) {
				;(stanza.content as BinaryNode[]).push({
					tag: 'device-identity',
					attrs: {},
					content: encodeSignedDeviceIdentity(authState.creds.account!, true)
				})

				logger.debug({ jid }, 'adding device identity')
			}

			// add buttons here
			const buttonType = getButtonType(message)
			if(buttonType) {
				(stanza.content as BinaryNode[]).push({
					tag: 'biz',
					attrs: { },
					content: [
						{
							tag: buttonType,
							attrs: getButtonArgs(message),
							...(buttonType !== 'list' && { content: getButtonContent(message) })
						}
					]
				})

				logger.debug({ jid }, 'adding business node')
			}
			// end of buttons add

			if (additionalNodes && additionalNodes.length > 0) {
				;(stanza.content as BinaryNode[]).push(...additionalNodes)
			}

			logger.debug({ msgId }, `sending message to ${participants.length} devices`)

			await sendNode(stanza)

			// Add message to retry cache if enabled
			if (messageRetryManager && !participant) {
				messageRetryManager.addRecentMessage(destinationJid, msgId, message)
			}
		}, meId)

		return msgId
	}

	const getMessageType = (message: proto.IMessage) => {
		if (message.pollCreationMessage || message.pollCreationMessageV2 || message.pollCreationMessageV3) {
			return 'poll'
		}

		if (message.eventMessage) {
			return 'event'
		}

		return 'text'
	}

	const getMediaType = (message: proto.IMessage) => {
		if (message.imageMessage) {
			return 'image'
		} else if (message.videoMessage) {
			return message.videoMessage.gifPlayback ? 'gif' : 'video'
		} else if (message.audioMessage) {
			return message.audioMessage.ptt ? 'ptt' : 'audio'
		} else if (message.contactMessage) {
			return 'vcard'
		} else if (message.documentMessage) {
			return 'document'
		} else if (message.contactsArrayMessage) {
			return 'contact_array'
		} else if (message.liveLocationMessage) {
			return 'livelocation'
		} else if (message.stickerMessage) {
			return 'sticker'
		} else if (message.listMessage) {
			return 'list'
		} else if (message.listResponseMessage) {
			return 'list_response'
		} else if (message.buttonsResponseMessage) {
			return 'buttons_response'
		} else if (message.orderMessage) {
			return 'order'
		} else if (message.productMessage) {
			return 'product'
		} else if (message.interactiveResponseMessage) {
			return 'native_flow_response'
		} else if (message.groupInviteMessage) {
			return 'url'
		}

		return ''
	}

	const getButtonType = (message: proto.IMessage) => {
		if(message.buttonsMessage) {
			return 'buttons'
		} else if(message.buttonsResponseMessage) {
			return 'buttons_response'
		} else if(message.interactiveResponseMessage) {
			return 'interactive_response'
		} else if(message.listMessage) {
			return 'list'
		} else if(message.listResponseMessage) {
			return 'list_response'
		} else if(message.interactiveMessage) {
			return 'interactive'
		}
	}

	const getButtonArgs = (message: proto.IMessage): BinaryNode['attrs'] => {
		if(message.templateMessage) {
			// TODO: Add attributes
			return {}
		} else if(message.listMessage) {
			return { v: '2', type: proto.Message.ListMessage.ListType[ListType.PRODUCT_LIST].toLowerCase() }
		} else if(message.interactiveMessage) {
            const buttons = message.interactiveMessage.nativeFlowMessage?.buttons || [];
            const hasPaymentInfoButton = buttons.some(button => button.name === "payment_info");
            
            if (hasPaymentInfoButton) {
                return {
                    v: "1",
                    type: "native_flow"
                };
            }
            
            return {
                type: "native_flow"
            };
        } 
		else {
			return {}
		}
	}

	const getButtonContent = (message: proto.IMessage):  BinaryNode['content'] => {
        if (!message) return []

        if (message.templateMessage) {
            // TODO: Add attributes
            return [];
        }
        else if (message.interactiveMessage) {
            const buttons = message.interactiveMessage.nativeFlowMessage?.buttons || [];
            const hasPaymentInfoButton = buttons.some(button => button.name === "payment_info");
            
            if (hasPaymentInfoButton) {
                return [{
                    tag: "native_flow",
                    attrs: {
                        name: "payment_info"
                    }
                }];
            } else {
                return [{
                    tag: "native_flow",
                    attrs: {
                        v: "2",
                        name: "mixed"
                    }
                }];
            }
        }
        else {
            return [];
        }
    }

	const getPrivacyTokens = async (jids: string[]) => {
		const t = unixTimestampSeconds().toString()
		const result = await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				xmlns: 'privacy'
			},
			content: [
				{
					tag: 'tokens',
					attrs: {},
					content: jids.map(jid => ({
						tag: 'token',
						attrs: {
							jid: jidNormalizedUser(jid),
							t,
							type: 'trusted_contact'
						}
					}))
				}
			]
		})

		return result
	}

	const waUploadToServer = getWAUploadToServer(config, refreshMediaConn)

	const waitForMsgMediaUpdate = bindWaitForEvent(ev, 'messages.media-update')

	return {
		...sock,
		getPrivacyTokens,
		assertSessions,
		relayMessage,
		sendReceipt,
		sendReceipts,
		readMessages,
		refreshMediaConn,
		waUploadToServer,
		fetchPrivacySettings,
		sendPeerDataOperationMessage,
		createParticipantNodes,
		getUSyncDevices,
		messageRetryManager,
		updateMediaMessage: async (message: WAMessage) => {
			const content = assertMediaContent(message.message)
			const mediaKey = content.mediaKey!
			const meId = authState.creds.me!.id
			const node = await encryptMediaRetryRequest(message.key, mediaKey, meId)

			let error: Error | undefined = undefined
			await Promise.all([
				sendNode(node),
				waitForMsgMediaUpdate(async update => {
					const result = update.find(c => c.key.id === message.key.id)
					if (result) {
						if (result.error) {
							error = result.error
						} else {
							try {
								const media = await decryptMediaRetryData(result.media!, mediaKey, result.key.id!)
								if (media.result !== proto.MediaRetryNotification.ResultType.SUCCESS) {
									const resultStr = proto.MediaRetryNotification.ResultType[media.result!]
									throw new Boom(`Media re-upload failed by device (${resultStr})`, {
										data: media,
										statusCode: getStatusCodeForMediaRetry(media.result!) || 404
									})
								}

								content.directPath = media.directPath
								content.url = getUrlFromDirectPath(content.directPath!)

								logger.debug({ directPath: media.directPath, key: result.key }, 'media update successful')
							} catch (err: any) {
								error = err
							}
						}

						return true
					}
				})
			])

			if (error) {
				throw error
			}

			ev.emit('messages.update', [{ key: message.key, update: { message: message.message } }])

			return message
		},
		sendMessage: async (jid: string, content: AnyMessageContent, options: MiscMessageGenerationOptions = {}) => {
			const userJid = authState.creds.me!.id
			if (
				typeof content === 'object' &&
				'disappearingMessagesInChat' in content &&
				typeof content['disappearingMessagesInChat'] !== 'undefined' &&
				isJidGroup(jid)
			) {
				const { disappearingMessagesInChat } = content
				const value =
					typeof disappearingMessagesInChat === 'boolean'
						? disappearingMessagesInChat
							? WA_DEFAULT_EPHEMERAL
							: 0
						: disappearingMessagesInChat
				await groupToggleEphemeral(jid, value)
			} else {
				const fullMsg = await generateWAMessage(jid, content, {
					logger,
				userJid,
				getUrlInfo: text =>
					getUrlInfo(text, {
						thumbnailWidth: linkPreviewImageThumbnailWidth,
						fetchOpts: {
							timeout: 3_000,
							...(axiosOptions || {})
						} as any,
						logger,
						uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined
					}),
				//TODO: CACHE
				getProfilePicUrl: sock.profilePictureUrl,
				getCallLink: sock.createCallLink,
				upload: waUploadToServer,
				mediaCache: config.mediaCache,
				options: config.options as any,
					transformAudio,
					messageId: generateMessageIDV2(sock.user?.id),
					...options
				})
				const isEventMsg = 'event' in content && !!content.event
				const isDeleteMsg = 'delete' in content && !!content.delete
				const isEditMsg = 'edit' in content && !!content.edit
				const isPinMsg = 'pin' in content && !!content.pin
				const isPollMessage = 'poll' in content && !!content.poll
				const additionalAttributes: BinaryNodeAttributes = {}
				const additionalNodes: BinaryNode[] = []
				// required for delete
				if (isDeleteMsg) {
					// if the chat is a group, and I am not the author, then delete the message as an admin
					if (isJidGroup(content.delete?.remoteJid as string) && !content.delete?.fromMe) {
						additionalAttributes.edit = '8'
					} else {
						additionalAttributes.edit = '7'
					}
				} else if (isEditMsg) {
					additionalAttributes.edit = '1'
				} else if (isPinMsg) {
					additionalAttributes.edit = '2'
				} else if (isPollMessage) {
					additionalNodes.push({
						tag: 'meta',
						attrs: {
							polltype: 'creation'
						}
					} as BinaryNode)
				} else if (isEventMsg) {
					additionalNodes.push({
						tag: 'meta',
						attrs: {
							event_type: 'creation'
						}
					} as BinaryNode)
				}

				const isGroup = isJidGroup(jid)
				
				if (isGroup && compatV6GroupSend) {
					logger.debug({ jid, compatV6: true }, '📤 Usando fluxo v6 simplificado para grupo')
					normalizeGroupJidsInContent(jid, content, options)
				}
				
				await relayMessage(jid, fullMsg.message!, {
					messageId: fullMsg.key.id!,
					useCachedGroupMetadata: options.useCachedGroupMetadata,
					additionalAttributes,
					statusJidList: options.statusJidList,
					additionalNodes
				})
				
				if (config.emitOwnEvents) {
					process.nextTick(() => {
						processingMutex.mutex(() => upsertMessage(fullMsg, 'append'))
					})
				}

				return fullMsg
			}
		}
	}
}
