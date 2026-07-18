package com.example.transfer.service

import com.example.transfer.transfer.TransferPauseState

enum class TransferNotificationAction { NONE, PAUSE, RESUME }

data class TransferNotificationModel(
    val title: String,
    val text: String,
    val showProgress: Boolean,
    val progress: Int = 0,
    val action: TransferNotificationAction = TransferNotificationAction.NONE
) {
    companion object {
        fun from(state: ServiceTransferState): TransferNotificationModel {
            val outgoing = state.outgoingTransfer
            val incoming = state.incomingTransfer
            return when {
                outgoing?.active == true && incoming?.active == true -> TransferNotificationModel(
                    "正在同时收发",
                    "发送 ${outgoing.batchProgress}% · 接收 ${incoming.batchProgress}%",
                    true,
                    outgoing.batchProgress,
                    outgoingAction(outgoing)
                )
                outgoing?.active == true -> singleDirection(
                    outgoing,
                    state.serviceMessage,
                    outgoingAction(outgoing)
                )
                incoming?.active == true -> singleDirection(
                    incoming,
                    state.serviceMessage,
                    TransferNotificationAction.NONE
                )
                outgoing != null -> singleDirection(
                    outgoing,
                    state.serviceMessage,
                    outgoingAction(outgoing)
                )
                incoming != null -> singleDirection(
                    incoming,
                    state.serviceMessage,
                    TransferNotificationAction.NONE
                )
                else -> idle()
            }
        }

        private fun idle() = TransferNotificationModel(
            "局域网互传运行中",
            "等待设备或文件",
            false
        )

        private fun singleDirection(
            transfer: ServiceTransfer,
            serviceMessage: String,
            action: TransferNotificationAction
        ): TransferNotificationModel {
            if (!transfer.active) return TransferNotificationModel(
                transfer.message,
                transfer.fileName.ifBlank { serviceMessage },
                false,
                transfer.progress
            )

            val text = if (transfer.fileCount > 1) {
                "第 ${transfer.fileIndex}/${transfer.fileCount} 个 · ${transfer.fileName} · ${transfer.batchProgress}%"
            } else {
                "${transfer.fileName} · ${transfer.progress}%"
            }
            return TransferNotificationModel(
                "${transfer.direction}文件",
                text,
                true,
                transfer.batchProgress,
                action
            )
        }

        private fun outgoingAction(transfer: ServiceTransfer): TransferNotificationAction = when {
            transfer.pauseState == TransferPauseState.PAUSING ||
                transfer.pauseState == TransferPauseState.PAUSED -> TransferNotificationAction.RESUME
            transfer.pauseState == TransferPauseState.RUNNING -> TransferNotificationAction.PAUSE
            else -> TransferNotificationAction.NONE
        }
    }
}
