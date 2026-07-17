package com.example.transfer.ui

import com.example.transfer.discovery.DiscoveredDevice
import com.example.transfer.service.ServiceTransferState
import com.example.transfer.service.ResumePrompt
import com.example.transfer.service.PairingOfferUi
import com.example.transfer.transfer.TransferPauseState

data class SelectedFile(
    val uri: String,
    val displayName: String,
    val mimeType: String,
    val size: Long,
    val lastModified: Long? = null
)

internal fun <T> validateThenPersist(
    validate: () -> T?,
    persist: () -> Unit
): T? {
    val validated = validate() ?: return null
    persist()
    return validated
}

internal class LatestSelectionRequest {
    private var generation = 0L

    fun nextToken(): Long {
        generation++
        return generation
    }

    fun nextTokenForSelection(itemCount: Int): Long? =
        if (itemCount > 0) nextToken() else null

    fun isLatest(token: Long): Boolean = token == generation

    fun completeIfLatest(
        token: Long,
        publish: () -> Unit,
        consume: () -> Unit
    ): Boolean {
        if (!isLatest(token)) return false
        publish()
        consume()
        return true
    }
}

internal class ResumePromptDisplayTracker {
    private val seenIds = mutableSetOf<Long>()

    fun shouldShow(promptId: Long): Boolean = seenIds.add(promptId)
}

internal data class PendingResumeConfirmation(
    val promptId: Long,
    val choice: com.example.transfer.service.ResumeChoice
)

internal class PendingResumeConfirmationController {
    private var sender: ((PendingResumeConfirmation) -> Unit)? = null
    private val acceptedPromptIds = mutableSetOf<Long>()
    var pending: PendingResumeConfirmation? = null
        private set

    fun attach(newSender: (PendingResumeConfirmation) -> Unit) {
        sender = newSender
        val queued = pending ?: return
        pending = null
        newSender(queued)
    }

    fun detach() {
        sender = null
    }

    fun confirm(promptId: Long, choice: com.example.transfer.service.ResumeChoice) {
        if (!acceptedPromptIds.add(promptId)) return
        val confirmation = PendingResumeConfirmation(promptId, choice)
        val currentSender = sender
        if (currentSender == null) pending = confirmation else currentSender(confirmation)
    }

    fun onPromptChanged(promptId: Long?) {
        if (pending?.promptId != null && pending?.promptId != promptId) pending = null
    }
}

data class TransferStatus(
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

data class TransferUiState(
    val devices: List<DiscoveredDevice> = emptyList(),
    val selectedDeviceId: String? = null,
    val preferredDeviceId: String? = null,
    val selectedFiles: List<SelectedFile> = emptyList(),
    val notice: String? = null,
    val serviceStatus: String = "正在启动接收服务…",
    val transfer: TransferStatus? = null,
    val resumePrompt: ResumePrompt? = null,
    val pairingOffer: PairingOfferUi? = null
) {
    val canSend: Boolean
        get() = selectedDeviceId != null &&
            devices.any { it.id == selectedDeviceId } &&
            selectedFiles.isNotEmpty() &&
            selectedFiles.all { it.size >= 0 } &&
            transfer?.active != true

    val canPause: Boolean
        get() = transfer?.let {
            it.active && it.direction == "发送" && it.pauseState == TransferPauseState.RUNNING
        } == true

    val canResume: Boolean
        get() = transfer?.let {
            it.active && it.direction == "发送" &&
                (it.pauseState == TransferPauseState.PAUSING || it.pauseState == TransferPauseState.PAUSED)
        } == true

    val canCancel: Boolean
        get() = transfer?.let {
            it.active && it.direction == "发送" && it.pauseState != TransferPauseState.CANCELLED
        } == true
}

object TransferUiReducer {
    fun withServiceState(state: TransferUiState, service: ServiceTransferState): TransferUiState {
        val withRecovery = service.recoverableBatch?.takeIf {
            state.selectedFiles.isEmpty()
        }?.let { recoverable ->
            state.copy(
                selectedFiles = recoverable.files,
                preferredDeviceId = recoverable.peerDeviceId,
                notice = null
            )
        } ?: state
        val preferredQr = service.preferredQrPeerId?.takeIf { id ->
            service.devices.any { it.id == id }
        }
        val withDevices = withDevices(withRecovery, service.devices).let { current ->
            if (preferredQr == null) current else current.copy(
                selectedDeviceId = preferredQr,
                preferredDeviceId = null
            )
        }
        val transfer = service.transfer?.let {
            TransferStatus(
                it.direction, it.fileName, it.progress, it.message, it.active,
                it.fileIndex, it.fileCount, it.batchProgress, it.pauseState
            )
        }
        return withDevices.copy(
            serviceStatus = service.serviceMessage,
            transfer = transfer,
            resumePrompt = service.resumePrompt,
            pairingOffer = service.pairingOffer
        )
    }

    fun withDevices(state: TransferUiState, devices: List<DiscoveredDevice>): TransferUiState {
        val selected = state.selectedDeviceId?.takeIf { id -> devices.any { it.id == id } }
            ?: state.preferredDeviceId?.takeIf { id -> devices.any { it.id == id } }
        return state.copy(devices = devices, selectedDeviceId = selected)
    }

    fun selectDevice(state: TransferUiState, id: String): TransferUiState =
        state.copy(
            selectedDeviceId = id.takeIf { candidate -> state.devices.any { it.id == candidate } },
            preferredDeviceId = null
        )

    fun restoreHistoryFile(
        state: TransferUiState,
        file: SelectedFile,
        preferredPeerId: String?
    ): TransferUiState {
        val preferred = preferredPeerId?.takeIf(String::isNotBlank)
        return state.copy(
            selectedDeviceId = preferred?.takeIf { id -> state.devices.any { it.id == id } },
            preferredDeviceId = preferred,
            selectedFiles = listOf(file),
            notice = null
        )
    }

    fun selectFiles(
        state: TransferUiState,
        files: List<SelectedFile>,
        notice: String? = null
    ): TransferUiState = state.copy(selectedFiles = files, notice = notice)
}
