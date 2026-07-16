package com.example.transfer.service

import java.util.concurrent.atomic.AtomicReference

internal class IncomingTransferAttemptTracker(
    private val acquire: () -> Boolean,
    private val release: () -> Unit
) {
    private sealed interface AttemptState {
        val attemptId: Long
    }

    private data class Active(override val attemptId: Long) : AttemptState
    private data class Finishing(override val attemptId: Long) : AttemptState

    private val current = AtomicReference<AttemptState?>(null)

    fun begin(attemptId: Long): Boolean {
        val active = Active(attemptId)
        if (!current.compareAndSet(null, active)) return false
        if (acquire()) return true
        current.compareAndSet(active, null)
        return false
    }

    fun isCurrent(attemptId: Long): Boolean = current.get().let { state ->
        state is Active && state.attemptId == attemptId
    }

    fun finish(attemptId: Long, beforeRelease: () -> Unit = {}): Boolean {
        val active = current.get() as? Active ?: return false
        if (active.attemptId != attemptId) return false
        val finishing = Finishing(attemptId)
        if (!current.compareAndSet(active, finishing)) return false
        var failure: Throwable? = null
        try {
            beforeRelease()
        } catch (error: Throwable) {
            failure = error
        } finally {
            try {
                release()
            } catch (error: Throwable) {
                if (failure == null) failure = error else failure.addSuppressed(error)
            } finally {
                current.compareAndSet(finishing, null)
            }
        }
        failure?.let { throw it }
        return true
    }
}
