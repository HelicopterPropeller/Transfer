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
