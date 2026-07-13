package com.example.transfer.service

interface ManagedLock {
    val isHeld: Boolean
    fun acquire()
    fun release()
}

class TransferResourceGuard(private vararg val locks: ManagedLock) {
    @Synchronized
    fun acquire() {
        if (locks.all { it.isHeld }) return
        try {
            locks.filterNot { it.isHeld }.forEach { it.acquire() }
        } catch (error: Exception) {
            release()
            throw error
        }
    }

    @Synchronized
    fun release() {
        locks.reversed().filter { it.isHeld }.forEach { runCatching { it.release() } }
    }
}
