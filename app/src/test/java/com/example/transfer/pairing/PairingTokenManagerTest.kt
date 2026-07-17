package com.example.transfer.pairing

import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class PairingTokenManagerTest {
    @Test
    fun `token is valid for exactly two minutes and can be consumed once`() {
        var now = 1_000L
        val manager = PairingTokenManager(clock = { now }, randomBytes = { ByteArray(it) { 7 } })

        val token = manager.issue()

        assertEquals(now + 120_000L, token.expiresAt)
        now = token.expiresAt - 1
        assertTrue(manager.consume(token.value))
        assertFalse(manager.consume(token.value))
    }

    @Test
    fun `refresh invalidation and expiry reject old tokens`() {
        var now = 10L
        var seed = 0
        val manager = PairingTokenManager(clock = { now }, randomBytes = { size ->
            ByteArray(size) { (++seed).toByte() }
        })
        val first = manager.issue()
        val second = manager.issue()
        assertNotEquals(first.value, second.value)
        assertFalse(manager.consume(first.value))

        manager.invalidate()
        assertFalse(manager.consume(second.value))

        val expired = manager.issue()
        now = expired.expiresAt
        assertFalse(manager.consume(expired.value))
    }

    @Test
    fun `concurrent consumers have exactly one winner`() {
        val manager = PairingTokenManager(randomBytes = { ByteArray(it) { 3 } })
        val token = manager.issue()
        val start = CountDownLatch(1)
        val results = Collections.synchronizedList(mutableListOf<Boolean>())
        val pool = Executors.newFixedThreadPool(8)
        repeat(8) {
            pool.execute {
                start.await()
                results += manager.consume(token.value)
            }
        }
        start.countDown()
        pool.shutdown()
        while (!pool.isTerminated) Thread.yield()

        assertEquals(1, results.count { it })
    }
}
