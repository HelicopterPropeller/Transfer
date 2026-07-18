package com.example.transfer.apkshare

import org.junit.Assert.assertFalse
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
}
