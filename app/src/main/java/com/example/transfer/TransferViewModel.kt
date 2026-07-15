package com.example.transfer

import android.app.Application
import android.os.Build
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.example.transfer.service.TransferServiceApi
import com.example.transfer.service.ResumeChoice
import com.example.transfer.ui.SelectedFile
import com.example.transfer.ui.PendingResumeConfirmationController
import com.example.transfer.ui.TransferUiReducer
import com.example.transfer.ui.TransferUiState
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

class TransferViewModel(application: Application) : AndroidViewModel(application) {
    val deviceName: String = listOf(Build.MANUFACTURER, Build.MODEL)
        .filter(String::isNotBlank).joinToString(" ").ifBlank { "Android 设备" }
    private val mutableState = MutableStateFlow(TransferUiState(serviceStatus = "正在连接后台服务…"))
    val state: StateFlow<TransferUiState> = mutableState.asStateFlow()
    private var service: TransferServiceApi? = null
    private var serviceCollection: Job? = null
    private val pendingResumeConfirmation = PendingResumeConfirmationController()

    fun attachService(newService: TransferServiceApi) {
        if (service === newService && serviceCollection?.isActive == true) return
        serviceCollection?.cancel()
        service = newService
        pendingResumeConfirmation.attach { confirmation ->
            newService.confirmResume(confirmation.promptId, confirmation.choice)
        }
        serviceCollection = viewModelScope.launch {
            newService.state.collect { serviceState ->
                pendingResumeConfirmation.onPromptChanged(serviceState.resumePrompt?.id)
                mutableState.update { TransferUiReducer.withServiceState(it, serviceState) }
            }
        }
    }

    fun detachService() {
        serviceCollection?.cancel()
        serviceCollection = null
        service = null
        pendingResumeConfirmation.detach()
    }

    fun selectDevice(id: String) {
        mutableState.update { TransferUiReducer.selectDevice(it, id) }
    }

    fun selectFiles(files: List<SelectedFile>, notice: String? = null) {
        mutableState.update { TransferUiReducer.selectFiles(it, files, notice) }
    }

    fun restoreHistoryFile(file: SelectedFile, preferredPeerId: String?) {
        mutableState.update { current ->
            TransferUiReducer.restoreHistoryFile(current, file, preferredPeerId).copy(
                notice = getApplication<Application>().getString(R.string.resend_file_ready)
            )
        }
    }

    fun showMessage(message: String) {
        mutableState.update { it.copy(notice = message) }
    }

    fun sendSelected() {
        val snapshot = mutableState.value
        val deviceId = snapshot.selectedDeviceId ?: return
        val files = snapshot.selectedFiles
        when {
            files.isEmpty() || files.any { it.size < 0 } ->
                showMessage("无法获取文件大小，请选择其他文件")
            service?.send(deviceId, files) != true ->
                showMessage("后台服务未连接或已有传输任务")
        }
    }

    fun togglePause() {
        val snapshot = mutableState.value
        when {
            snapshot.canPause -> service?.pause()
            snapshot.canResume -> service?.resume()
        }
    }

    fun confirmResume(promptId: Long, choice: ResumeChoice) {
        pendingResumeConfirmation.confirm(promptId, choice)
    }

    override fun onCleared() {
        detachService()
        super.onCleared()
    }
}
