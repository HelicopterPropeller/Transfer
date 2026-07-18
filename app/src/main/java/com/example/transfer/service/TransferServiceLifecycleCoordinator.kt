package com.example.transfer.service

internal class TransferServiceLifecycleCoordinator(
    private val lanes: TransferLaneGate,
) {
    fun beginOutgoing(): Boolean = lanes.begin(TransferLane.OUTGOING)

    fun endOutgoing(): Boolean = lanes.end(TransferLane.OUTGOING)

    fun beginIncoming(): Boolean = lanes.begin(TransferLane.INCOMING)

    fun endIncoming(): Boolean = lanes.end(TransferLane.INCOMING)

    fun isOutgoingActive(): Boolean = lanes.isActive(TransferLane.OUTGOING)

    fun isIncomingActive(): Boolean = lanes.isActive(TransferLane.INCOMING)

    fun drain() = lanes.closeAll()
}
