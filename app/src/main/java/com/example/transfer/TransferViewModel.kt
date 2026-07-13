package com.example.transfer

import android.app.Application
import android.net.Uri
import android.os.Build
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.example.transfer.service.TransferServiceApi
import com.example.transfer.ui.SelectedFile
import com.example.transfer.ui.TransferStatus
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

    fun attachService(newService: TransferServiceApi) {
        if (service === newService && serviceCollection?.isActive == true) return
        serviceCollection?.cancel()
        service = newService
        serviceCollection = viewModelScope.launch {
            newService.state.collect { serviceState ->
                mutableState.update { TransferUiReducer.withServiceState(it, serviceState) }
            }
        }
    }

    fun detachService() {
        serviceCollection?.cancel()
        serviceCollection = null
        service = null
    }

    fun selectDevice(id: String) {
        mutableState.update { TransferUiReducer.selectDevice(it, id) }
    }

    fun selectFile(uri: Uri, name: String, mimeType: String?, size: Long) {
        mutableState.update {
            it.copy(selectedFile = SelectedFile(
                uri.toString(), name,
                mimeType.orEmpty().ifBlank { "application/octet-stream" }, size
            ))
        }
    }

    fun showMessage(message: String) {
        mutableState.update { it.copy(transfer = TransferStatus("", "", 0, message, false)) }
    }

    fun sendSelected() {
        val snapshot = mutableState.value
        val deviceId = snapshot.selectedDeviceId ?: return
        val file = snapshot.selectedFile ?: return
        when {
            file.size < 0 -> showMessage("无法获取文件大小，请选择其他文件")
            service?.send(deviceId, file) != true -> showMessage("后台服务未连接或已有传输任务")
        }
    }

    override fun onCleared() {
        detachService()
        super.onCleared()
    }
}
