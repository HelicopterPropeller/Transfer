package com.example.transfer.apkshare

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class ApkShareSessionTest {
    @Test
    fun `failed attempt can retry but complete attempt consumes session`() {
        var now = 1_000L
        val session = ApkShareSession.create(
            nowMillis = { now },
            randomBytes = { ByteArray(24) { 7 } },
        )

        assertTrue(session.authorize(session.token))
        val first = session.beginAttempt(session.token)!!
        session.failAttempt(first)
        val second = session.beginAttempt(session.token)!!
        assertTrue(session.completeAttempt(second))
        assertFalse(session.authorize(session.token))
    }

    @Test
    fun `session expires after ten minutes`() {
        var now = 5_000L
        val session = ApkShareSession.create(
            nowMillis = { now },
            randomBytes = { ByteArray(24) },
        )

        now += 600_000L

        assertFalse(session.authorize(session.token))
    }

    @Test
    fun `attempt cannot complete at expiry and is released`() {
        var now = 10_000L
        val session = ApkShareSession.create(
            nowMillis = { now },
            randomBytes = { ByteArray(24) },
        )
        val attempt = session.beginAttempt(session.token)!!

        now = session.expiresAtMillis

        assertFalse(session.completeAttempt(attempt))
        now = session.expiresAtMillis - 1L
        assertNotNull(session.beginAttempt(session.token))
    }

    @Test
    fun `attempt cannot complete after expiry`() {
        var now = 20_000L
        val session = ApkShareSession.create(
            nowMillis = { now },
            randomBytes = { ByteArray(24) },
        )
        val attempt = session.beginAttempt(session.token)!!

        now = session.expiresAtMillis + 1L

        assertFalse(session.completeAttempt(attempt))
    }

    @Test
    fun `token uses 24 random bytes as 48 lowercase hex characters`() {
        var requestedSize = 0
        val session = ApkShareSession.create(
            nowMillis = { 30_000L },
            randomBytes = { size ->
                requestedSize = size
                ByteArray(size) { index -> (index * 11).toByte() }
            },
        )

        assertEquals(24, requestedSize)
        assertTrue(session.token.matches(Regex("^[0-9a-f]{48}$")))
    }

    @Test
    fun `create rejects random byte arrays that are not exactly 24 bytes`() {
        listOf(0, 23, 25).forEach { size ->
            assertThrows(IllegalArgumentException::class.java) {
                ApkShareSession.create(
                    nowMillis = { 40_000L },
                    randomBytes = { ByteArray(size) },
                )
            }
        }
    }
}
