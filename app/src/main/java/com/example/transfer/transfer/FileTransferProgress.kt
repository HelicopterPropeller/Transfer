package com.example.transfer.transfer

data class FileTransferProgress(
    val confirmedBytes: Long,
    val totalBytes: Long
) {
    val percent: Int
        get() = TransferProgress.percent(confirmedBytes, totalBytes)
}
