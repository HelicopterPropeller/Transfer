package com.example.transfer.service

import java.util.concurrent.atomic.AtomicBoolean

internal enum class TransferLane { OUTGOING, INCOMING }

internal class TransferLaneGate(
    private val acquireResources: () -> Unit,
    private val releaseResources: () -> Unit,
) {
    private val outgoingActive = AtomicBoolean(false)
    private val incomingActive = AtomicBoolean(false)

    fun begin(lane: TransferLane): Boolean {
        val active = activeState(lane)
        if (!active.compareAndSet(false, true)) return false

        return try {
            acquireResources()
            true
        } catch (_: Exception) {
            active.set(false)
            false
        }
    }

    fun end(lane: TransferLane): Boolean {
        if (!activeState(lane).compareAndSet(true, false)) return false

        releaseResources()
        return true
    }

    fun isActive(lane: TransferLane): Boolean = activeState(lane).get()

    private fun activeState(lane: TransferLane): AtomicBoolean = when (lane) {
        TransferLane.OUTGOING -> outgoingActive
        TransferLane.INCOMING -> incomingActive
    }
}
