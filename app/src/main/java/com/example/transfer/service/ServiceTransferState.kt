package com.example.transfer.service

import com.example.transfer.discovery.DiscoveredDevice
import com.example.transfer.transfer.TransferPauseState

data class ServiceTransfer(
    val direction: String,
    val fileName: String,
    val progress: Int,
    val message: String,
    val active: Boolean,
    val fileIndex: Int = 1,
    val fileCount: Int = 1,
    val batchProgress: Int = progress,
    val pauseState: TransferPauseState = TransferPauseState.RUNNING
)

data class ServiceTransferState(
    val devices: List<DiscoveredDevice> = emptyList(),
    val serviceMessage: String = "等待设备或文件",
    val transfer: ServiceTransfer? = null
)

internal class ServiceTerminationGate {
    private val lock = Any()
    private var closed = false

    fun close(): Boolean = synchronized(lock) {
        if (closed) false else {
            closed = true
            true
        }
    }

    fun closeWithAction(action: () -> Unit): Boolean = synchronized(lock) {
        if (closed) false else {
            try {
                action()
            } finally {
                closed = true
            }
            true
        }
    }

    fun runIfOpen(action: () -> Unit): Boolean = synchronized(lock) {
        if (closed) false else {
            action()
            true
        }
    }
}

internal fun checkedBatchLength(lengths: Iterable<Long>): Long? {
    var total = 0L
    for (length in lengths) {
        if (length < 0) return null
        total = try {
            Math.addExact(total, length)
        } catch (_: ArithmeticException) {
            return null
        }
    }
    return total
}

internal fun ServiceTransferState.withLatestOutgoingPauseState(
    controllerMatches: Boolean,
    latestPauseState: () -> TransferPauseState
): ServiceTransferState {
    val current = transfer
    if (
        !controllerMatches ||
        current == null ||
        !current.active ||
        current.direction != "发送"
    ) return this
    val pauseState = latestPauseState()
    return copy(transfer = current.copy(
        message = servicePauseMessage(pauseState),
        pauseState = pauseState
    ))
}

internal fun ServiceTransferState.withInactiveBatchFailure(message: String): ServiceTransferState {
    val current = transfer
    return copy(transfer = if (current == null) {
        ServiceTransfer("发送", "", 0, message, false)
    } else {
        current.copy(message = message, active = false)
    })
}

internal fun servicePauseMessage(state: TransferPauseState): String = when (state) {
    TransferPauseState.RUNNING -> "正在发送"
    TransferPauseState.PAUSING -> "正在暂停"
    TransferPauseState.PAUSED -> "已暂停"
    TransferPauseState.CANCELLED -> "正在取消"
}
