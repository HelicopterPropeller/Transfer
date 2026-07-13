package com.example.transfer

import android.app.Application
import android.net.Uri
import android.os.Build
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.example.transfer.discovery.DiscoveryManager
import com.example.transfer.storage.DownloadStorage
import com.example.transfer.transfer.FileTransferClient
import com.example.transfer.transfer.FileTransferServer
import com.example.transfer.transfer.SendFileSource
import com.example.transfer.ui.SelectedFile
import com.example.transfer.ui.TransferStatus
import com.example.transfer.ui.TransferUiReducer
import com.example.transfer.ui.TransferUiState
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import java.util.UUID

class TransferViewModel(application: Application) : AndroidViewModel(application) {
    private val app = application
    private val storage = DownloadStorage(app)
    private val client = FileTransferClient()
    private val sendMutex = Mutex()
    private val deviceId = app.getSharedPreferences("transfer", 0)
        .let { preferences ->
            preferences.getString("device_id", null) ?: UUID.randomUUID().toString().also {
                preferences.edit().putString("device_id", it).apply()
            }
        }
    val deviceName: String = listOf(Build.MANUFACTURER, Build.MODEL)
        .filter { it.isNotBlank() }
        .joinToString(" ")
        .ifBlank { "Android 设备" }
    private val discovery = DiscoveryManager(app, deviceId, deviceName)
    private val server = FileTransferServer(store = storage)
    private var foreground = false

    private val mutableState = MutableStateFlow(TransferUiState())
    val state: StateFlow<TransferUiState> = mutableState.asStateFlow()

    fun startForeground() {
        if (foreground) return
        foreground = true
        mutableState.update { it.copy(serviceStatus = "正在启动接收服务…") }
        server.start(
            scope = viewModelScope,
            onStarted = { mutableState.update { state -> state.copy(serviceStatus = "可接收 · TCP $it") } },
            onProgress = { fileName, progress ->
                mutableState.update {
                    it.copy(transfer = TransferStatus("接收", fileName, progress, "正在接收", true))
                }
            },
            onComplete = { fileName ->
                mutableState.update {
                    it.copy(transfer = TransferStatus("接收", fileName, 100, "已保存到 Download/Transfer", false))
                }
            },
            onError = { message ->
                mutableState.update {
                    it.copy(transfer = TransferStatus("接收", "", 0, message, false))
                }
            }
        )
        discovery.start(
            scope = viewModelScope,
            onDevices = { devices -> mutableState.update { TransferUiReducer.withDevices(it, devices) } },
            onError = { message -> mutableState.update { it.copy(serviceStatus = message) } }
        )
    }

    fun stopForeground() {
        if (!foreground) return
        foreground = false
        discovery.stop()
        server.stop()
        mutableState.update {
            TransferUiReducer.withDevices(it, emptyList()).copy(serviceStatus = "已暂停，返回前台后继续")
        }
    }

    fun selectDevice(id: String) {
        mutableState.update { TransferUiReducer.selectDevice(it, id) }
    }

    fun selectFile(uri: Uri, name: String, mimeType: String?, size: Long) {
        mutableState.update {
            it.copy(
                selectedFile = SelectedFile(
                    uri.toString(),
                    name,
                    mimeType.orEmpty().ifBlank { "application/octet-stream" },
                    size
                )
            )
        }
    }

    fun showMessage(message: String) {
        mutableState.update {
            it.copy(transfer = TransferStatus("", "", 0, message, false))
        }
    }

    fun sendSelected() {
        val snapshot = mutableState.value
        val device = snapshot.devices.firstOrNull { it.id == snapshot.selectedDeviceId } ?: return
        val file = snapshot.selectedFile ?: return
        if (file.size < 0) {
            showMessage("无法获取文件大小，请选择其他文件")
            return
        }
        if (!sendMutex.tryLock()) return
        mutableState.update {
            it.copy(transfer = TransferStatus("发送", file.displayName, 0, "正在连接 ${device.name}", true))
        }
        viewModelScope.launch {
            try {
                val source = SendFileSource(file.displayName, file.mimeType, file.size) {
                    app.contentResolver.openInputStream(Uri.parse(file.uri))
                        ?: error("无法读取所选文件")
                }
                val result = client.send(device.address, device.port, source) { progress ->
                    mutableState.update {
                        it.copy(transfer = TransferStatus("发送", file.displayName, progress, "正在发送", true))
                    }
                }
                mutableState.update {
                    it.copy(
                        transfer = if (result.isSuccess) {
                            TransferStatus("发送", file.displayName, 100, "发送完成", false)
                        } else {
                            TransferStatus("发送", file.displayName, 0, result.exceptionOrNull()?.message ?: "发送失败", false)
                        }
                    )
                }
            } finally {
                sendMutex.unlock()
            }
        }
    }

    override fun onCleared() {
        discovery.stop()
        server.stop()
        super.onCleared()
    }
}
