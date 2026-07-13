package com.example.transfer.service

import android.app.Service
import android.content.Intent
import android.net.wifi.WifiManager
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import androidx.core.content.edit
import androidx.core.net.toUri
import com.example.transfer.discovery.DiscoveryManager
import com.example.transfer.storage.DownloadStorage
import com.example.transfer.transfer.FileTransferClient
import com.example.transfer.transfer.FileTransferServer
import com.example.transfer.transfer.SendFileSource
import com.example.transfer.ui.SelectedFile
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

class TransferForegroundService : Service() {
    inner class LocalBinder : Binder(), TransferServiceApi {
        override val state: StateFlow<ServiceTransferState>
            get() = this@TransferForegroundService.state
        override fun send(deviceId: String, file: SelectedFile): Boolean =
            this@TransferForegroundService.send(deviceId, file)
    }

    private val binder = LocalBinder()
    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val busy = AtomicBoolean(false)
    private val mutableState = MutableStateFlow(ServiceTransferState())
    val state: StateFlow<ServiceTransferState> = mutableState.asStateFlow()
    private lateinit var notificationFactory: TransferNotificationFactory
    private lateinit var resourceGuard: TransferResourceGuard
    private lateinit var discovery: DiscoveryManager
    private lateinit var server: FileTransferServer
    private val client = FileTransferClient()

    override fun onCreate() {
        super.onCreate()
        notificationFactory = TransferNotificationFactory(this).also { it.createChannel() }
        startForeground(
            TransferNotificationFactory.NOTIFICATION_ID,
            notificationFactory.build(TransferNotificationModel.from(mutableState.value))
        )
        resourceGuard = createResourceGuard()
        val deviceId = getSharedPreferences("transfer", 0).let { preferences ->
            preferences.getString("device_id", null) ?: UUID.randomUUID().toString().also {
                preferences.edit { putString("device_id", it) }
            }
        }
        val deviceName = listOf(Build.MANUFACTURER, Build.MODEL).filter(String::isNotBlank)
            .joinToString(" ").ifBlank { "Android 设备" }
        discovery = DiscoveryManager(this, deviceId, deviceName)
        server = FileTransferServer(
            store = DownloadStorage(this),
            onTransferStart = ::beginTransfer,
            onTransferEnd = ::endTransfer
        )
        server.start(
            serviceScope,
            { publish { it.copy(serviceMessage = "后台接收服务运行中 · TCP $it") } },
            { name, progress -> publish { it.copy(transfer = ServiceTransfer("接收", name, progress, "正在接收", true)) } },
            { name -> publish { it.copy(transfer = ServiceTransfer("接收", name, 100, "接收完成", false)) } },
            { message -> publish {
                if (it.transfer?.active == true) it
                else it.copy(transfer = ServiceTransfer("接收", "", 0, message, false))
            } }
        )
        discovery.start(
            serviceScope,
            { devices -> publish { it.copy(devices = devices) } },
            { message -> publish { it.copy(serviceMessage = message) } }
        )
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            shutdown()
            stopForeground(STOP_FOREGROUND_REMOVE)
            stopSelf()
            return START_NOT_STICKY
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder = binder

    fun send(deviceId: String, file: SelectedFile): Boolean {
        val device = state.value.devices.firstOrNull { it.id == deviceId } ?: return false
        if (file.size < 0 || !beginTransfer()) return false
        publish { it.copy(transfer = ServiceTransfer("发送", file.displayName, 0, "正在连接 ${device.name}", true)) }
        serviceScope.launch {
            try {
                val source = SendFileSource(file.displayName, file.mimeType, file.size) {
                    contentResolver.openInputStream(file.uri.toUri()) ?: error("无法读取所选文件")
                }
                val result = client.send(device.address, device.port, source) { progress ->
                    publish { it.copy(transfer = ServiceTransfer("发送", file.displayName, progress, "正在发送", true)) }
                }
                publish {
                    it.copy(transfer = if (result.isSuccess) {
                        ServiceTransfer("发送", file.displayName, 100, "发送完成", false)
                    } else {
                        ServiceTransfer("发送", file.displayName, 0, result.exceptionOrNull()?.message ?: "发送失败", false)
                    })
                }
            } finally {
                endTransfer()
            }
        }
        return true
    }

    private fun beginTransfer(): Boolean {
        if (!busy.compareAndSet(false, true)) return false
        return runCatching { resourceGuard.acquire(); true }.getOrElse { busy.set(false); false }
    }

    private fun endTransfer() {
        resourceGuard.release()
        busy.set(false)
    }

    private fun publish(transform: (ServiceTransferState) -> ServiceTransferState) {
        mutableState.update(transform)
        startForeground(
            TransferNotificationFactory.NOTIFICATION_ID,
            notificationFactory.build(TransferNotificationModel.from(mutableState.value))
        )
    }

    private fun createResourceGuard(): TransferResourceGuard {
        val wakeLock = (getSystemService(POWER_SERVICE) as PowerManager)
            .newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "$packageName:transfer")
            .apply { setReferenceCounted(false) }
        @Suppress("DEPRECATION")
        val wifiLock = (applicationContext.getSystemService(WIFI_SERVICE) as WifiManager)
            .createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "$packageName:transfer")
            .apply { setReferenceCounted(false) }
        return TransferResourceGuard(
            object : ManagedLock {
                override val isHeld get() = wakeLock.isHeld
                override fun acquire() = wakeLock.acquire(MAX_LOCK_MILLIS)
                override fun release() = wakeLock.release()
            },
            object : ManagedLock {
                override val isHeld get() = wifiLock.isHeld
                override fun acquire() = wifiLock.acquire()
                override fun release() = wifiLock.release()
            }
        )
    }

    private fun shutdown() {
        client.cancelActive()
        if (::server.isInitialized) server.stop()
        if (::discovery.isInitialized) discovery.stop()
        if (::resourceGuard.isInitialized) resourceGuard.release()
        serviceScope.cancel()
        busy.set(false)
    }

    override fun onDestroy() {
        shutdown()
        super.onDestroy()
    }

    override fun onTimeout(startId: Int, fgsType: Int) {
        publish { it.copy(transfer = ServiceTransfer("", "", 0, "后台服务达到系统运行时限", false)) }
        shutdown()
        stopSelf()
    }

    companion object {
        const val ACTION_START = "com.example.transfer.action.START"
        const val ACTION_STOP = "com.example.transfer.action.STOP"
        private const val MAX_LOCK_MILLIS = 12L * 60 * 60 * 1000
    }
}
