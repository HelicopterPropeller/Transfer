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
import com.example.transfer.history.HistoryPeer
import com.example.transfer.history.OutgoingHistoryRecorder
import com.example.transfer.history.TransferHistoryDatabase
import com.example.transfer.history.TransferHistoryRepository
import com.example.transfer.storage.DownloadStorage
import com.example.transfer.transfer.FileTransferClient
import com.example.transfer.transfer.FileTransferServer
import com.example.transfer.transfer.SendFileSource
import com.example.transfer.transfer.TransferBatchRunner
import com.example.transfer.transfer.TransferPauseController
import com.example.transfer.transfer.TransferPauseState
import com.example.transfer.transfer.formatBatchCompletion
import com.example.transfer.ui.SelectedFile
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.util.UUID
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean

class TransferForegroundService : Service() {
    inner class LocalBinder : Binder(), TransferServiceApi {
        override val state: StateFlow<ServiceTransferState>
            get() = this@TransferForegroundService.state
        override fun send(deviceId: String, files: List<SelectedFile>): Boolean =
            this@TransferForegroundService.send(deviceId, files)
        override fun pause(): Boolean = this@TransferForegroundService.pause()
        override fun resume(): Boolean = this@TransferForegroundService.resume()
    }

    private val binder = LocalBinder()
    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val busy = AtomicBoolean(false)
    private val cancellationStarted = AtomicBoolean(false)
    private val terminationGate = ServiceTerminationGate()
    private val mutableState = MutableStateFlow(ServiceTransferState())
    val state: StateFlow<ServiceTransferState> = mutableState.asStateFlow()
    private lateinit var notificationFactory: TransferNotificationFactory
    private lateinit var resourceGuard: TransferResourceGuard
    private lateinit var discovery: DiscoveryManager
    private lateinit var server: FileTransferServer
    private lateinit var historyRepository: TransferHistoryRepository
    private lateinit var outgoingHistoryRecorder: OutgoingHistoryRecorder
    private lateinit var historyStartupGate: HistoryStartupGate
    private val client = FileTransferClient()
    @Volatile private var activePauseController: TransferPauseController? = null
    @Volatile private var activeBatchJob: Job? = null

    override fun onCreate() {
        super.onCreate()
        historyRepository = TransferHistoryRepository(
            TransferHistoryDatabase.getInstance(this).transferHistoryDao()
        )
        outgoingHistoryRecorder = OutgoingHistoryRecorder(historyRepository)
        historyStartupGate = HistoryStartupGate(serviceScope) {
            historyRepository.interruptActive()
        }
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
        if (intent?.action == ACTION_PAUSE) pause()
        if (intent?.action == ACTION_RESUME) resume()
        if (intent?.action == ACTION_STOP) {
            shutdown()
            stopForeground(STOP_FOREGROUND_REMOVE)
            stopSelf()
            return START_NOT_STICKY
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder = binder

    fun send(deviceId: String, files: List<SelectedFile>): Boolean {
        if (files.isEmpty() || checkedBatchLength(files.map { it.size }) == null) return false
        var accepted = false
        terminationGate.runIfOpen gate@{
            val device = state.value.devices.firstOrNull { it.id == deviceId } ?: return@gate
            if (!beginTransfer()) return@gate
            val sources = files.map { file ->
                SendFileSource(
                    displayName = file.displayName,
                    mimeType = file.mimeType,
                    length = file.size,
                    sourceUri = file.uri
                ) {
                    contentResolver.openInputStream(file.uri.toUri()) ?: error("无法读取所选文件")
                }
            }
            val controller = TransferPauseController()
            activePauseController = controller
            publish { current ->
                if (activePauseController !== controller) current else {
                    val pauseState = controller.state
                    current.copy(transfer = ServiceTransfer(
                        direction = "发送",
                        fileName = files.first().displayName,
                        progress = 0,
                        message = if (pauseState == TransferPauseState.RUNNING) {
                            "正在连接 ${device.name}"
                        } else {
                            servicePauseMessage(pauseState)
                        },
                        active = true,
                        fileIndex = 1,
                        fileCount = files.size,
                        batchProgress = 0,
                        pauseState = pauseState
                    ))
                }
            }
            lateinit var job: Job
            val cleanupStarted = AtomicBoolean(false)
            val cleanup = {
                if (cleanupStarted.compareAndSet(false, true)) {
                    if (activePauseController === controller) activePauseController = null
                    if (activeBatchJob === job) activeBatchJob = null
                    endTransfer()
                }
            }
            job = serviceScope.launch(start = CoroutineStart.LAZY) {
                try {
                    historyStartupGate.awaitReady()
                    val onPauseState: (TransferPauseState) -> Unit = {
                        publishPauseState(controller)
                    }
                    val runner = TransferBatchRunner(controller) { source, onProgress ->
                        outgoingHistoryRecorder.send(
                            source = source,
                            peer = HistoryPeer(
                                id = device.id,
                                name = device.name,
                                address = device.address.hostAddress
                            ),
                            controller = controller
                        ) {
                            client.send(
                                device.address,
                                device.port,
                                source,
                                controller,
                                onPauseState,
                                onProgress
                            )
                        }
                    }
                    val result = runner.run(
                        sources,
                        onProgress = { progress ->
                            publish { current ->
                                if (
                                    activePauseController !== controller ||
                                    current.transfer?.active != true
                                ) {
                                    current
                                } else {
                                    val pauseState = controller.state
                                    current.copy(transfer = ServiceTransfer(
                                        direction = "发送",
                                        fileName = progress.fileName,
                                        progress = progress.fileProgress,
                                        message = servicePauseMessage(pauseState),
                                        active = true,
                                        fileIndex = progress.fileIndex,
                                        fileCount = progress.fileCount,
                                        batchProgress = progress.batchProgress,
                                        pauseState = pauseState
                                    ))
                                }
                            }
                        },
                        onPauseState = onPauseState
                    )
                    publish { current ->
                        if (activePauseController !== controller) current else {
                            current.copy(transfer = ServiceTransfer(
                                direction = "发送",
                                fileName = files.last().displayName,
                                progress = 100,
                                message = formatBatchCompletion(result),
                                active = false,
                                fileIndex = files.size,
                                fileCount = files.size,
                                batchProgress = 100,
                                pauseState = controller.state
                            ))
                        }
                    }
                } catch (exception: CancellationException) {
                    throw exception
                } catch (exception: Exception) {
                    publish { current ->
                        if (activePauseController !== controller) current else {
                            current.withInactiveBatchFailure(
                                "发送失败：${exception.message ?: "未知错误"}"
                            )
                        }
                    }
                } finally {
                    cleanup()
                }
            }
            activeBatchJob = job
            job.invokeOnCompletion { cleanup() }
            accepted = job.start()
            if (!accepted) cleanup()
        }
        return accepted
    }

    fun pause(): Boolean {
        val controller = activePauseController ?: return false
        if (!controller.requestPause()) return false
        publishPauseState(controller)
        return true
    }

    fun resume(): Boolean {
        val controller = activePauseController ?: return false
        return controller.requestResume()
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
        terminationGate.runIfOpen {
            publishNow(transform)
        }
    }

    private fun publishNow(transform: (ServiceTransferState) -> ServiceTransferState) {
        mutableState.update(transform)
        startForeground(
            TransferNotificationFactory.NOTIFICATION_ID,
            notificationFactory.build(TransferNotificationModel.from(mutableState.value))
        )
    }

    private fun publishPauseState(controller: TransferPauseController) {
        publish { current ->
            current.withLatestOutgoingPauseState(
                controllerMatches = activePauseController === controller,
                latestPauseState = { controller.state }
            )
        }
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
        terminationGate.close()
        cancelResourcesOnce()
    }

    private fun cancelResourcesOnce() {
        if (!cancellationStarted.compareAndSet(false, true)) return
        activePauseController?.cancel()
        client.cancelActive()
        if (::server.isInitialized) server.stop()
        if (::discovery.isInitialized) discovery.stop()
        activeBatchJob?.cancel()
        serviceScope.cancel()
    }

    override fun onDestroy() {
        shutdown()
        super.onDestroy()
    }

    override fun onTimeout(startId: Int, fgsType: Int) {
        try {
            terminationGate.closeWithAction {
                publishNow {
                    it.copy(transfer = ServiceTransfer("", "", 0, "后台服务达到系统运行时限", false))
                }
            }
        } finally {
            cancelResourcesOnce()
            stopSelf()
        }
    }

    companion object {
        const val ACTION_START = "com.example.transfer.action.START"
        const val ACTION_PAUSE = "com.example.transfer.action.PAUSE"
        const val ACTION_RESUME = "com.example.transfer.action.RESUME"
        const val ACTION_STOP = "com.example.transfer.action.STOP"
        private const val MAX_LOCK_MILLIS = 12L * 60 * 60 * 1000
    }
}
