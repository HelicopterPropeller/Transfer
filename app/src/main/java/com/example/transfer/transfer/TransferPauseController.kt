package com.example.transfer.transfer

import java.util.concurrent.CancellationException

enum class TransferPauseState { RUNNING, PAUSING, PAUSED, CANCELLED }

class TransferPauseController {
    private val lock = Object()
    private var resumeRequested = false

    @Volatile
    var state = TransferPauseState.RUNNING
        private set

    fun requestPause(): Boolean = synchronized(lock) {
        if (state != TransferPauseState.RUNNING) {
            false
        } else {
            state = TransferPauseState.PAUSING
            true
        }
    }

    fun requestResume(): Boolean = synchronized(lock) {
        if (state != TransferPauseState.PAUSING && state != TransferPauseState.PAUSED) {
            false
        } else {
            resumeRequested = true
            lock.notifyAll()
            true
        }
    }

    fun checkpoint(
        sendPause: () -> Unit,
        sendResume: () -> Unit,
        onState: (TransferPauseState) -> Unit
    ) {
        synchronized(lock) {
            if (state != TransferPauseState.PAUSING) return
        }
        sendPause()
        var notifyPaused = false
        synchronized(lock) {
            if (state == TransferPauseState.CANCELLED) {
                throw CancellationException("Transfer cancelled")
            }
            if (!resumeRequested) {
                state = TransferPauseState.PAUSED
                notifyPaused = true
            }
        }
        if (notifyPaused) onState(TransferPauseState.PAUSED)
        synchronized(lock) {
            while (!resumeRequested && state != TransferPauseState.CANCELLED) lock.wait()
            if (state == TransferPauseState.CANCELLED) {
                throw CancellationException("Transfer cancelled")
            }
            resumeRequested = false
        }
        sendResume()
        synchronized(lock) {
            state = TransferPauseState.RUNNING
        }
        onState(TransferPauseState.RUNNING)
    }

    fun awaitBetweenFiles(onState: (TransferPauseState) -> Unit) {
        var notifyPaused = false
        synchronized(lock) {
            if (state != TransferPauseState.PAUSING) return
            if (!resumeRequested) {
                state = TransferPauseState.PAUSED
                notifyPaused = true
            }
        }
        if (notifyPaused) onState(TransferPauseState.PAUSED)
        synchronized(lock) {
            while (!resumeRequested && state != TransferPauseState.CANCELLED) lock.wait()
            if (state == TransferPauseState.CANCELLED) {
                throw CancellationException("Transfer cancelled")
            }
            resumeRequested = false
            state = TransferPauseState.RUNNING
        }
        onState(TransferPauseState.RUNNING)
    }

    fun cancel() = synchronized(lock) {
        state = TransferPauseState.CANCELLED
        lock.notifyAll()
    }
}
