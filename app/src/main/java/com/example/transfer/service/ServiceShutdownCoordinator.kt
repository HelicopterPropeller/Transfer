package com.example.transfer.service

import java.util.concurrent.atomic.AtomicBoolean

internal class ServiceShutdownCoordinator {
    private val started = AtomicBoolean(false)

    fun shutdown(
        cancelOutgoing: () -> Unit,
        preventNewStarts: () -> Unit,
        stopResources: () -> Unit
    ) {
        if (!started.compareAndSet(false, true)) return
        try {
            cancelOutgoing()
        } finally {
            try {
                preventNewStarts()
            } finally {
                stopResources()
            }
        }
    }
}
