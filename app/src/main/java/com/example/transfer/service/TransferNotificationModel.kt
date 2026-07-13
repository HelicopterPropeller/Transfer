package com.example.transfer.service

data class TransferNotificationModel(
    val title: String,
    val text: String,
    val showProgress: Boolean,
    val progress: Int = 0
) {
    companion object {
        fun from(state: ServiceTransferState): TransferNotificationModel {
            val transfer = state.transfer ?: return TransferNotificationModel(
                "局域网互传运行中", "等待设备或文件", false
            )
            if (transfer.active) return TransferNotificationModel(
                "${transfer.direction}文件",
                "${transfer.fileName} · ${transfer.progress}%",
                true,
                transfer.progress
            )
            return TransferNotificationModel(
                transfer.message,
                transfer.fileName.ifBlank { state.serviceMessage },
                false,
                transfer.progress
            )
        }
    }
}
