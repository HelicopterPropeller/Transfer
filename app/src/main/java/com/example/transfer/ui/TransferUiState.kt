package com.example.transfer.ui

import com.example.transfer.discovery.DiscoveredDevice

data class SelectedFile(
    val uri: String,
    val displayName: String,
    val mimeType: String,
    val size: Long
)

data class TransferStatus(
    val direction: String,
    val fileName: String,
    val progress: Int,
    val message: String,
    val active: Boolean
)

data class TransferUiState(
    val devices: List<DiscoveredDevice> = emptyList(),
    val selectedDeviceId: String? = null,
    val selectedFile: SelectedFile? = null,
    val serviceStatus: String = "正在启动接收服务…",
    val transfer: TransferStatus? = null
) {
    val canSend: Boolean
        get() = selectedDeviceId != null &&
            devices.any { it.id == selectedDeviceId } &&
            selectedFile != null &&
            transfer?.active != true
}

object TransferUiReducer {
    fun withDevices(state: TransferUiState, devices: List<DiscoveredDevice>): TransferUiState {
        val selected = state.selectedDeviceId?.takeIf { id -> devices.any { it.id == id } }
        return state.copy(devices = devices, selectedDeviceId = selected)
    }

    fun selectDevice(state: TransferUiState, id: String): TransferUiState =
        state.copy(selectedDeviceId = id.takeIf { candidate -> state.devices.any { it.id == candidate } })
}
