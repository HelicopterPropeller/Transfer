package com.example.transfer.service

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class IncomingTransferAttemptTrackerTest {
    @Test
    fun `terminal UI update completes before busy release and a new attempt can start`() {
        val terminalEntered = CountDownLatch(1)
        val releaseTerminal = CountDownLatch(1)
        var uiAttempt: Long? = 1L
        var releases = 0
        val tracker = IncomingTransferAttemptTracker(
            acquire = { true },
            release = { releases++ }
        )
        assertTrue(tracker.begin(1))
        val pool = Executors.newSingleThreadExecutor()
        try {
            val finishing = pool.submit<Boolean> {
                tracker.finish(1) {
                    terminalEntered.countDown()
                    check(releaseTerminal.await(2, TimeUnit.SECONDS))
                    uiAttempt = null
                }
            }
            assertTrue(terminalEntered.await(2, TimeUnit.SECONDS))

            assertFalse(tracker.begin(2))
            assertEquals(0, releases)

            releaseTerminal.countDown()
            assertTrue(finishing.get(2, TimeUnit.SECONDS))
            assertEquals(null, uiAttempt)
            assertEquals(1, releases)

            assertTrue(tracker.begin(2))
            uiAttempt = 2L
            assertFalse(tracker.finish(1) { uiAttempt = null })
            assertEquals(2L, uiAttempt)
        } finally {
            releaseTerminal.countDown()
            pool.shutdownNow()
        }
    }

    @Test
    fun `stale terminal callback cannot finish a newer incoming attempt`() {
        var acquired = 0
        var released = 0
        val tracker = IncomingTransferAttemptTracker(
            acquire = { acquired++; true },
            release = { released++ }
        )

        assertTrue(tracker.begin(1))
        assertTrue(tracker.finish(1))
        assertTrue(tracker.begin(2))

        assertFalse(tracker.finish(1))
        assertTrue(tracker.isCurrent(2))
        assertEquals(2, acquired)
        assertEquals(1, released)
    }

    @Test
    fun `concurrent starts admit exactly one attempt and acquire resources once`() {
        val start = CountDownLatch(1)
        var acquired = 0
        val tracker = IncomingTransferAttemptTracker(
            acquire = { synchronized(this) { acquired++ }; true },
            release = {}
        )
        val pool = Executors.newFixedThreadPool(2)
        try {
            val results = listOf(10L, 11L).map { id ->
                pool.submit<Boolean> {
                    start.await()
                    tracker.begin(id)
                }
            }
            start.countDown()

            assertEquals(1, results.count { it.get() })
            assertEquals(1, acquired)
        } finally {
            pool.shutdownNow()
        }
    }
}
