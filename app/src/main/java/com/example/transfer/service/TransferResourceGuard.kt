package com.example.transfer.service

interface ManagedLock {
    val isHeld: Boolean
    fun acquire()
    fun release()
}

class TransferResourceGuard(private vararg val locks: ManagedLock) {
    private var leaseCount = 0

    @Synchronized
    fun acquire() {
        if (leaseCount > 0) {
            leaseCount++
            return
        }
        try {
            locks.filterNot { it.isHeld }.forEach { it.acquire() }
        } catch (error: Exception) {
            releaseLocks()
            throw error
        }
        leaseCount = 1
    }

    @Synchronized
    fun release() {
        if (leaseCount == 0) return
        leaseCount--
        if (leaseCount > 0) return
        releaseLocks()
    }

    private fun releaseLocks() {
        locks.reversed().filter { it.isHeld }.forEach { runCatching { it.release() } }
    }
}
