package com.example.transfer.service

import org.junit.Assert.assertEquals
import org.junit.Test

class TransferResourceGuardTest {
    @Test
    fun `two leases keep locks held until both are released`() {
        val releaseOrder = mutableListOf<String>()
        val first = FakeLock("first", releaseOrder)
        val second = FakeLock("second", releaseOrder)
        val guard = TransferResourceGuard(first, second)

        guard.acquire()
        guard.acquire()

        assertEquals(1, first.acquireCount)
        assertEquals(1, second.acquireCount)
        guard.release()
        assertEquals(0, first.releaseCount)
        assertEquals(0, second.releaseCount)

        guard.release()

        assertEquals(1, first.releaseCount)
        assertEquals(1, second.releaseCount)
        assertEquals(listOf("second", "first"), releaseOrder)
    }

    @Test
    fun `extra release is harmless`() {
        val lock = FakeLock("only", mutableListOf())
        val guard = TransferResourceGuard(lock)

        guard.acquire()
        guard.release()
        guard.release()

        assertEquals(1, lock.acquireCount)
        assertEquals(1, lock.releaseCount)
    }

    private class FakeLock(
        private val name: String,
        private val releaseOrder: MutableList<String>
    ) : ManagedLock {
        override var isHeld = false
        var acquireCount = 0
        var releaseCount = 0
        override fun acquire() { isHeld = true; acquireCount++ }
        override fun release() {
            isHeld = false
            releaseCount++
            releaseOrder += name
        }
    }
}
