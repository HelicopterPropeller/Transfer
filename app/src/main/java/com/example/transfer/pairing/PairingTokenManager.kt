package com.example.transfer.pairing

import java.security.MessageDigest
import java.security.SecureRandom

data class PairingToken(val value: String, val expiresAt: Long)

class PairingTokenManager(
    private val clock: () -> Long = System::currentTimeMillis,
    private val randomBytes: (Int) -> ByteArray = { size ->
        ByteArray(size).also(SecureRandom()::nextBytes)
    }
) {
    private var active: PairingToken? = null

    @Synchronized
    fun issue(): PairingToken {
        val token = PairingToken(
            value = encodeHex(randomBytes(TOKEN_BYTES)),
            expiresAt = clock() + VALIDITY_MILLIS
        )
        active = token
        return token
    }

    @Synchronized
    fun invalidate() {
        active = null
    }

    @Synchronized
    fun consume(candidate: String): Boolean {
        val token = active ?: return false
        if (clock() >= token.expiresAt) {
            active = null
            return false
        }
        val matches = MessageDigest.isEqual(
            token.value.toByteArray(Charsets.UTF_8),
            candidate.toByteArray(Charsets.UTF_8)
        )
        if (matches) active = null
        return matches
    }

    companion object {
        const val TOKEN_BYTES = 16
        const val VALIDITY_MILLIS = 120_000L
    }

    private fun encodeHex(bytes: ByteArray): String {
        val digits = "0123456789abcdef"
        return CharArray(bytes.size * 2) { index ->
            val value = bytes[index / 2].toInt() and 0xff
            digits[if (index % 2 == 0) value ushr 4 else value and 0x0f]
        }.concatToString()
    }
}
