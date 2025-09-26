import { LRUCache } from 'lru-cache'
import type { SignalKeyStoreWithTransaction } from '../Types'
import type { ILogger } from '../Utils/logger'
import { isLidUser, isPnUser, jidDecode } from '../WABinary'

export class LIDMappingStore {
	private readonly mappingCache = new LRUCache<string, string>({
		ttl: 7 * 24 * 60 * 60 * 1000, // 7 days
		ttlAutopurge: true,
		updateAgeOnGet: true
	})
	private readonly keys: SignalKeyStoreWithTransaction
	private readonly logger: ILogger

	constructor(keys: SignalKeyStoreWithTransaction, logger: ILogger) {
		this.keys = keys
		this.logger = logger
	}

	/**
	 * Store LID-PN mapping - USER LEVEL
	 */
	async storeLIDPNMappings(pairs: { lid: string; pn: string }[]): Promise<void> {
		// Validate inputs
		const pairMap: { [_: string]: string } = {}
		for (const { lid, pn } of pairs) {
			if (!((isLidUser(lid) && isPnUser(pn)) || (isPnUser(lid) && isLidUser(pn)))) {
				this.logger.warn(`Invalid LID-PN mapping: ${lid}, ${pn}`)
				continue
			}
			const [lidJid, pnJid] = isLidUser(lid) ? [lid, pn] : [pn, lid]
			const lidDecoded = jidDecode(lidJid)
			const pnDecoded = jidDecode(pnJid)

			if (!lidDecoded || !pnDecoded) return

			const pnUser = pnDecoded.user
			const lidUser = lidDecoded.user

			// Check if mapping already exists (cache first, then database)
			let existingLidUser = this.mappingCache.get(`pn:${pnUser}`)
			if (!existingLidUser) {
				this.logger.trace(`Cache miss for PN user ${pnUser}; checking database`)
				const stored = await this.keys.get('lid-mapping', [pnUser])
				existingLidUser = stored[pnUser]
				if (existingLidUser) {
					// Update cache with database value
					this.mappingCache.set(`pn:${pnUser}`, existingLidUser)
					this.mappingCache.set(`lid:${existingLidUser}`, pnUser)
				}
			}

			if (existingLidUser === lidUser) {
				this.logger.debug({ pnUser, lidUser }, 'LID mapping already exists, skipping')
				continue
			}

			pairMap[pnUser] = lidUser
		}

		this.logger.trace({ pairMap }, `Storing ${Object.keys(pairMap).length} pn mappings`)

		await this.keys.transaction(async () => {
			for (const [pnUser, lidUser] of Object.entries(pairMap)) {
				await this.keys.set({
					'lid-mapping': {
						[pnUser]: lidUser,
						[`${lidUser}_reverse`]: pnUser
					}
				})

				this.mappingCache.set(`pn:${pnUser}`, lidUser)
				this.mappingCache.set(`lid:${lidUser}`, pnUser)
			}
		}, 'lid-mapping')
	}

	/**
	 * Get LID for PN - Returns user-level LID without device separation
	 */
	async getLIDForPN(pn: string): Promise<string | null> {
		if (!isPnUser(pn)) return null

		const decoded = jidDecode(pn)
		if (!decoded) return null

		// Look up user-level mapping (whatsmeow approach)
		const pnUser = decoded.user
		let lidUser = this.mappingCache.get(`pn:${pnUser}`)

		if (!lidUser) {
			// Cache miss - check database
			const stored = await this.keys.get('lid-mapping', [pnUser])
			lidUser = stored[pnUser]

			if (lidUser) {
				// Cache the database result
				this.mappingCache.set(`pn:${pnUser}`, lidUser)
				this.mappingCache.set(`lid:${lidUser}`, pnUser)
			} else {
				this.logger.trace(`No LID mapping found for PN user ${pnUser}`)
				return null
			}
		}

		lidUser = lidUser.toString()
		if (!lidUser) {
			this.logger.warn(`Invalid or empty LID user for PN ${pn}: lidUser = "${lidUser}"`)
			return null
		}

		// Return simple LID without device separation to keep conversations unified
		const unifiedLid = `${lidUser}@lid`

		this.logger.trace(`getLIDForPN: ${pn} → ${unifiedLid} (unified user mapping)`)
		return unifiedLid
	}

	/**
	 * Get PN for LID - Returns simple PN without device construction
	 */
	async getPNForLID(lid: string): Promise<string | null> {
		if (!isLidUser(lid)) return null

		const decoded = jidDecode(lid)
		if (!decoded) return null

		// Look up reverse user mapping
		const lidUser = decoded.user
		let pnUser = this.mappingCache.get(`lid:${lidUser}`)

		if (!pnUser || typeof pnUser !== 'string') {
			// Cache miss - check database
			const stored = await this.keys.get('lid-mapping', [`${lidUser}_reverse`])
			pnUser = stored[`${lidUser}_reverse`]

			if (!pnUser || typeof pnUser !== 'string') {
				this.logger.trace(`No reverse mapping found for LID user: ${lidUser}`)
				return null
			}

			this.mappingCache.set(`lid:${lidUser}`, pnUser)
		}

		// Return simple PN JID without device separation to keep conversations unified
		const pnJid = `${pnUser}@s.whatsapp.net`

		this.logger.trace(`Found reverse mapping: ${lid} → ${pnJid} (unified)`)
		return pnJid
	}
}
