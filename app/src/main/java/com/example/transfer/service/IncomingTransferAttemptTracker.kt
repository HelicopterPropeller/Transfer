package com.example.transfer.service

import java.util.concurrent.atomic.AtomicReference

internal class IncomingTransferAttemptTracker(
    private val acquire: () -> Boolean,
    private val release: () -> Unit
) {
    private val current = AtomicReference<Long?>(null)

    fun begin(attemptId: Long): Boolean {
        if (!current.compareAndSet(null, attemptId)) return false
        if (acquire()) return true
        current.compareAndSet(attemptId, null)
        return false
    }

    fun isCurrent(attemptId: Long): Boolean = current.get() == attemptId

    fun finish(attemptId: Long): Boolean {
        if (!current.compareAndSet(attemptId, null)) return false
        release()
        return true
    }
}
