package com.example.transfer.service

import com.example.transfer.discovery.DiscoveredDevice

data class ServiceTransfer(
    val direction: String,
    val fileName: String,
    val progress: Int,
    val message: String,
    val active: Boolean
)

data class ServiceTransferState(
    val devices: List<DiscoveredDevice> = emptyList(),
    val serviceMessage: String = "等待设备或文件",
    val transfer: ServiceTransfer? = null
)
