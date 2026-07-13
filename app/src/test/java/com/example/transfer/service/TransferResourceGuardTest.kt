package com.example.transfer.service

import org.junit.Assert.assertEquals
import org.junit.Test

class TransferResourceGuardTest {
    @Test
    fun `acquire and release are idempotent`() {
        val first = FakeLock()
        val second = FakeLock()
        val guard = TransferResourceGuard(first, second)
        guard.acquire(); guard.acquire(); guard.release(); guard.release()
        assertEquals(1, first.acquireCount)
        assertEquals(1, second.acquireCount)
        assertEquals(1, first.releaseCount)
        assertEquals(1, second.releaseCount)
    }

    private class FakeLock : ManagedLock {
        override var isHeld = false
        var acquireCount = 0
        var releaseCount = 0
        override fun acquire() { isHeld = true; acquireCount++ }
        override fun release() { isHeld = false; releaseCount++ }
    }
}
