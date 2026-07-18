package com.example.transfer.apkshare

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom

class ApkShareSession private constructor(
    val token: String,
    val expiresAtMillis: Long,
    private val nowMillis: () -> Long,
) {
    private var activeAttempt: Long? = null
    private var nextAttempt = 1L
    private var consumed = false

    @Synchronized
    fun authorize(candidate: String): Boolean =
        !consumed &&
            nowMillis() < expiresAtMillis &&
            MessageDigest.isEqual(
                token.toByteArray(StandardCharsets.UTF_8),
                candidate.toByteArray(StandardCharsets.UTF_8),
            )

    @Synchronized
    fun beginAttempt(candidate: String): Long? {
        if (!authorize(candidate) || activeAttempt != null) return null
        return nextAttempt++.also { activeAttempt = it }
    }

    @Synchronized
    fun failAttempt(id: Long) {
        if (activeAttempt == id) activeAttempt = null
    }

    @Synchronized
    fun completeAttempt(id: Long): Boolean {
        if (activeAttempt != id) return false
        activeAttempt = null
        if (nowMillis() >= expiresAtMillis) return false
        consumed = true
        return true
    }

    companion object {
        fun create(
            nowMillis: () -> Long = System::currentTimeMillis,
            randomBytes: (Int) -> ByteArray = { size ->
                ByteArray(size).also(SecureRandom()::nextBytes)
            },
        ): ApkShareSession {
            val bytes = randomBytes(24)
            require(bytes.size == 24) { "randomBytes must return exactly 24 bytes" }
            val token = bytes.joinToString("") {
                "%02x".format(it.toInt() and 0xff)
            }
            return ApkShareSession(token, nowMillis() + 600_000L, nowMillis)
        }
    }
}
