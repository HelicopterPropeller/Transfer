package com.example.transfer.service

import com.example.transfer.ui.SelectedFile
import kotlinx.coroutines.flow.StateFlow

interface TransferServiceApi {
    val state: StateFlow<ServiceTransferState>
    fun send(deviceId: String, files: List<SelectedFile>): Boolean
    fun send(deviceId: String, file: SelectedFile): Boolean = send(deviceId, listOf(file))
    fun confirmResume(promptId: Long, choice: ResumeChoice): Boolean
    fun pause(): Boolean
    fun resume(): Boolean
    fun cancelOutgoing(): Boolean = false
    fun createPairingOffer(): Boolean
    fun dismissPairingOffer(rawPayload: String): Boolean
    fun connectQr(rawPayload: String): Boolean
    fun acknowledgeQrPeer(deviceId: String): Boolean
}
