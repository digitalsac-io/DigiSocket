const { makeWASocket, DisconnectReason, useMultiFileAuthState } = require('../lib')
const { Boom } = require('@hapi/boom')

async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys')
    const sock = makeWASocket({
        // default: auth from default store
        auth: state,
        printQRInTerminal: true,
        // NEW FEATURE: Enable automatic audio transformation to PTT format
        transformAudio: true, // Set to false to disable
    })

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update
        if(connection === 'close') {
            const shouldReconnect = (lastDisconnect.error)?.output?.statusCode !== DisconnectReason.loggedOut
            console.log('connection closed due to ', lastDisconnect.error, ', reconnecting ', shouldReconnect)
            // reconnect if not logged out
            if(shouldReconnect) {
                connectToWhatsApp()
            }
        } else if(connection === 'open') {
            console.log('opened connection')
        }
    })

    sock.ev.on('creds.update', saveCreds)

    // Example: Send an audio file as PTT
    // The audio will be automatically converted to the format required by WhatsApp
    await sock.sendMessage("5511999999999@s.whatsapp.net", {
        audio: { url: './meu-audio.mp3' }, // Any audio format supported by ffmpeg
        ptt: true, // This triggers the automatic conversion when transformAudio: true
        mimetype: 'audio/mp4' // Original format, will be converted to audio/ogg; codecs=opus
    })

    console.log("✅ Audio sent! Process flow:")
    console.log("📁 Input: meu-audio.mp3")
    console.log("📄 Temp original: /tmp/audioXXXXX-original")
    console.log("🔄 ffmpeg -i INPUT -avoid_negative_ts make_zero -ac 1 -c:a libopus OUTPUT_ptt.ogg")
    console.log("📄 Temp converted: /tmp/audioXXXXX_ptt.ogg")
    console.log("🚀 Upload converted file")
    console.log("🗑️ Clean up all temp files")
    console.log("✨ PTT message delivered!")
}

// Usage examples:
console.log("=== Baileys Audio Transform Feature ===")
console.log("To enable audio transformation, set transformAudio: true in your socket config")
console.log("This will automatically convert any audio to PTT-compatible format (opus codec)")
console.log("")
console.log("Example configuration:")
console.log(`
const sock = makeWASocket({
    auth: state,
    transformAudio: true, // Enable automatic audio conversion
})
`)
console.log("")
console.log("When sending audio with ptt: true, it will be automatically converted using:")
console.log("ffmpeg -i ARQUIVO -avoid_negative_ts make_zero -ac 1 -c:a libopus out.ogg")

// connectToWhatsApp()
