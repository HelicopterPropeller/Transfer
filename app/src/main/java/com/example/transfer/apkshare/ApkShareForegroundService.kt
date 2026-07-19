package com.example.transfer.apkshare

import android.Manifest
import android.app.Service
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Binder
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import androidx.core.content.ContextCompat
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

class ApkShareForegroundService : Service() {
    inner class LocalBinder : Binder() {
        val state: StateFlow<ApkShareState>
            get() = this@ApkShareForegroundService.state

        fun startExistingLan() = this@ApkShareForegroundService.startExistingLan()
        fun startAutomaticHotspot() = this@ApkShareForegroundService.startAutomaticHotspot()
        fun requestManualHotspotFallback() =
            this@ApkShareForegroundService.requestManualHotspotFallback()

        fun continueAfterManualHotspot() =
            this@ApkShareForegroundService.continueAfterManualHotspot()

        fun showDownloadQr() = this@ApkShareForegroundService.showDownloadQr()
        fun stopIfIdle() = this@ApkShareForegroundService.stopIfIdle()
        fun cancel() = this@ApkShareForegroundService.cancelAndStop()
    }

    private class ActiveOperation(
        val generation: Long,
        var retry: RetryAction,
    ) {
        var artifact: ApkArtifact? = null
        var workspace: ApkPreparationWorkspace? = null
        var hotspot: HotspotController? = null
        var hotspotBefore: List<InterfaceAddressSnapshot>? = null
        var server: LocalApkHttpServer? = null
        var session: ApkShareSession? = null
        var downloadReadyState: ApkShareState.ReadyToDownload? = null
        var timeoutJob: Job? = null
        val servingRequested = AtomicBoolean()
        private val closed = AtomicBoolean()
        private val coordinator = ApkShareCoordinator(
            closeServer = { server?.close() },
            closeHotspot = { hotspot?.close() },
            deleteArtifact = { workspace?.clear() ?: artifact?.file?.delete() },
        )

        fun close() {
            if (!closed.compareAndSet(false, true)) return
            timeoutJob?.cancel()
            coordinator.close()
        }
    }

    private val binder = LocalBinder()
    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val cleanupExecutor = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "apk-share-cleanup")
    }
    private val mainHandler = Handler(Looper.getMainLooper())
    private val operationLock = Any()
    private val mutableState = MutableStateFlow<ApkShareState>(ApkShareState.Idle)
    private val notificationGate = ApkShareNotificationGate()
    private val notificationRunnable = Runnable {
        if (!notificationGate.beginDelivery()) return@Runnable
        runCatching {
            startForeground(
                ApkShareNotificationFactory.NOTIFICATION_ID,
                notificationFactory.build(mutableState.value),
            )
        }
    }
    val state: StateFlow<ApkShareState> = mutableState.asStateFlow()
    private lateinit var notificationFactory: ApkShareNotificationFactory
    private var nextGeneration = 0L
    private var activeOperation: ActiveOperation? = null

    override fun onCreate() {
        super.onCreate()
        notificationFactory = ApkShareNotificationFactory(this).also { it.createChannel() }
        startForeground(
            ApkShareNotificationFactory.NOTIFICATION_ID,
            notificationFactory.build(ApkShareState.Idle),
        )
    }

    override fun onBind(intent: Intent?): IBinder = binder

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_CANCEL -> cancelAndStop()
            ACTION_STOP_IF_IDLE -> stopIfIdle()
        }
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        notificationGate.close()
        mainHandler.removeCallbacks(notificationRunnable)
        serviceScope.cancel()
        closeAsync(detachActive())
        cleanupExecutor.shutdown()
        super.onDestroy()
    }

    private fun startExistingLan() {
        val operation = beginOperation(RetryAction.PREPARE_EXISTING_LAN)
        serviceScope.launch {
            val address = ApkShareAddressResolver.existingWifi(
                InterfaceAddressProvider.snapshot(this@ApkShareForegroundService),
            )
            if (address == null) {
                fail(operation, "未找到可用的 Wi-Fi 局域网地址")
                return@launch
            }
            val artifact = prepareArtifact(operation) ?: return@launch
            serve(operation, address, artifact)
        }
    }

    private fun startAutomaticHotspot() {
        val requirement = HotspotPermissionPolicy.requirementFor(Build.VERSION.SDK_INT)
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            cancelActiveAndPublish(
                ApkShareState.ManualHotspotRequired(HotspotFailure.GENERIC),
            )
            return
        }
        if (!hasPermission(requirement)) {
            cancelActiveAndPublish(ApkShareState.PermissionRequired(requirement))
            return
        }

        val operation = beginOperation(RetryAction.PREPARE_HOTSPOT)
        serviceScope.launch {
            prepareArtifact(operation) ?: return@launch
            if (!publishFor(operation, ApkShareState.StartingHotspot)) return@launch

            val controller = HotspotController(this@ApkShareForegroundService)
            if (!attachHotspot(operation, controller)) {
                controller.close()
                return@launch
            }
            controller.start(
                onLost = {
                    fail(operation, "本地热点已关闭，请重试或使用手动热点")
                },
            ) { result ->
                when (result) {
                    is HotspotStartResult.Started -> {
                        val accepted = synchronized(operationLock) {
                            if (activeOperation !== operation) {
                                false
                            } else {
                                operation.hotspotBefore = result.before
                                true
                            }
                        }
                        if (accepted) {
                            publishFor(operation, ApkShareState.JoinHotspot(result.credentials))
                        }
                    }

                    is HotspotStartResult.ManualRequired -> {
                        releaseHotspot(operation)
                        publishFor(
                            operation,
                            ApkShareState.ManualHotspotRequired(result.failure),
                        )
                    }
                }
            }
        }
    }

    private fun requestManualHotspotFallback() {
        cancelActiveAndPublish(
            ApkShareState.ManualHotspotRequired(HotspotFailure.USER_REJECTED),
        )
    }

    private fun continueAfterManualHotspot() {
        val reusable = synchronized(operationLock) {
            activeOperation?.takeIf { operation ->
                operation.artifact != null &&
                    mutableState.value is ApkShareState.ManualHotspotRequired
            }?.also { operation ->
                operation.retry = RetryAction.RESOLVE_MANUAL_HOTSPOT
            }
        }
        if (reusable != null) {
            serviceScope.launch { resolveManualAndServe(reusable) }
            return
        }

        val operation = beginOperation(RetryAction.RESOLVE_MANUAL_HOTSPOT)
        serviceScope.launch {
            prepareArtifact(operation) ?: return@launch
            resolveManualAndServe(operation)
        }
    }

    private suspend fun resolveManualAndServe(operation: ActiveOperation) {
        val snapshots = InterfaceAddressProvider.snapshot(this)
        val address = ApkShareAddressResolver.newHotspotAddress(emptyList(), snapshots)
            ?: ApkShareAddressResolver.existingWifi(snapshots)
        if (address == null) {
            fail(operation, "未找到手动热点的局域网地址")
            return
        }
        val artifact = synchronized(operationLock) {
            operation.artifact.takeIf { activeOperation === operation }
        } ?: return
        serve(operation, address, artifact)
    }

    private fun showDownloadQr() {
        val operation = synchronized(operationLock) {
            activeOperation?.takeIf {
                mutableState.value is ApkShareState.JoinHotspot &&
                    it.hotspotBefore != null &&
                    it.artifact != null
            }
        } ?: return
        if (!operation.servingRequested.compareAndSet(false, true)) return

        serviceScope.launch {
            val before = synchronized(operationLock) {
                operation.hotspotBefore?.takeIf { activeOperation === operation }
            } ?: return@launch
            val address = ApkShareAddressResolver.newHotspotAddress(
                before,
                InterfaceAddressProvider.snapshot(this@ApkShareForegroundService),
            )
            if (address == null) {
                fail(operation, "未找到新热点的局域网地址")
                return@launch
            }
            val artifact = synchronized(operationLock) {
                operation.artifact.takeIf { activeOperation === operation }
            } ?: return@launch
            serve(operation, address, artifact)
        }
    }

    private fun cancelAndStop() {
        val detached = synchronized(operationLock) {
            val current = activeOperation
            activeOperation = null
            mutableState.value = ApkShareState.Cancelled
            current
        }
        notificationGate.close()
        mainHandler.removeCallbacks(notificationRunnable)
        serviceScope.cancel()
        closeAsync(detached)
        cleanupExecutor.shutdown()
        stopForegroundAndSelf()
    }

    private fun stopIfIdle() {
        val shouldStop = synchronized(operationLock) {
            ApkShareServiceStopPolicy.shouldStopOnUiExit(
                mutableState.value,
                activeOperation != null,
            )
        }
        if (shouldStop) stopForegroundAndSelf()
    }

    private fun stopForegroundAndSelf() {
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun beginOperation(retry: RetryAction): ActiveOperation {
        val old: ActiveOperation?
        val operation: ActiveOperation
        synchronized(operationLock) {
            old = activeOperation
            operation = ActiveOperation(++nextGeneration, retry)
            activeOperation = operation
            mutableState.value = ApkShareState.PreparingApk
            scheduleNotificationLocked()
        }
        closeAsync(old)
        return operation
    }

    private fun prepareArtifact(operation: ActiveOperation): ApkArtifact? {
        val workspaceDirectory = File(
            cacheDir,
            "$APK_SHARE_CACHE_DIRECTORY/${operation.generation}",
        )
        val workspace = ApkPreparationWorkspace(workspaceDirectory)
        val mayPrepare = synchronized(operationLock) {
            if (activeOperation !== operation) {
                false
            } else {
                operation.workspace = workspace
                true
            }
        }
        if (!mayPrepare) {
            workspace.clear()
            return null
        }
        val result = runCatching {
            InstalledApkSource().prepare(this, workspaceDirectory)
        }.getOrElse { exception ->
            ApkPreparationResult.Failure(
                exception.message ?: "无法读取当前安装包信息",
            )
        }
        val artifact = when (result) {
            is ApkPreparationResult.Ready -> result.artifact
            ApkPreparationResult.SplitInstallUnsupported -> {
                workspace.clear()
                fail(operation, "当前安装包含拆分 APK，无法离线分享")
                return null
            }

            is ApkPreparationResult.Failure -> {
                workspace.clear()
                fail(operation, result.message)
                return null
            }
        }
        val attached = synchronized(operationLock) {
            if (activeOperation !== operation) {
                false
            } else {
                operation.artifact = artifact
                true
            }
        }
        if (!attached) workspace.clear()
        return artifact.takeIf { attached }
    }

    private fun serve(
        operation: ActiveOperation,
        address: SelectedLanAddress,
        artifact: ApkArtifact,
    ) {
        val session = ApkShareSession.create()
        val server = LocalApkHttpServer(
            callbackExecutor = ContextCompat.getMainExecutor(this),
        )
        val attached = synchronized(operationLock) {
            if (activeOperation !== operation || operation.server != null) {
                false
            } else {
                operation.server = server
                operation.session = session
                true
            }
        }
        if (!attached) {
            server.close()
            return
        }

        val listener = listenerFor(operation)
        val port = try {
            server.start(address, artifact, session, listener)
        } catch (exception: Exception) {
            fail(operation, exception.message ?: "无法启动安装包下载服务")
            return
        }
        val url = "http://${address.address.hostAddress}:$port/i/${session.token}/"
        if (!publishDownloadReadyFor(
                operation,
                ApkShareState.ReadyToDownload(url, artifact, session.expiresAtMillis),
            )) {
            return
        }
        val timer = serviceScope.launch {
            delay((session.expiresAtMillis - System.currentTimeMillis()).coerceAtLeast(0L))
            val stillExactSession = synchronized(operationLock) {
                activeOperation === operation && operation.session === session
            }
            if (stillExactSession) {
                fail(operation, "安装包分享链接已过期")
            }
        }
        synchronized(operationLock) {
            if (activeOperation === operation && operation.session === session) {
                operation.timeoutJob = timer
            } else {
                timer.cancel()
            }
        }
    }

    private fun listenerFor(operation: ActiveOperation): ApkDownloadListener =
        RetryableApkDownloadListener(
            readyState = {
                synchronized(operationLock) {
                    operation.downloadReadyState.takeIf { activeOperation === operation }
                }
            },
            publish = { state -> publishFor(operation, state) },
            complete = { completeOperation(operation) },
            onCompletedPublished = {
                operation.timeoutJob?.cancel()
                closeAsync(operation)
                serviceScope.launch {
                    delay(COMPLETION_CLEANUP_DELAY_MILLIS)
                    val shouldStop = synchronized(operationLock) {
                        ApkShareServiceStopPolicy.shouldStopAfterCompletion(
                            mutableState.value,
                            activeOperation != null,
                        )
                    }
                    if (shouldStop) stopForegroundAndSelf()
                }
            },
        )

    private fun completeOperation(operation: ActiveOperation): Boolean =
        synchronized(operationLock) {
            if (activeOperation !== operation) return false
            activeOperation = null
            mutableState.value = ApkShareState.Completed
            scheduleNotificationLocked()
            true
        }

    private fun fail(operation: ActiveOperation, message: String) {
        val detached = synchronized(operationLock) {
            if (activeOperation !== operation) return
            activeOperation = null
            mutableState.value = ApkShareState.Error(message, operation.retry)
            scheduleNotificationLocked()
            operation
        }
        closeAsync(detached)
    }

    private fun publishFor(operation: ActiveOperation, state: ApkShareState): Boolean =
        synchronized(operationLock) {
            if (activeOperation !== operation) return false
            mutableState.value = state
            scheduleNotificationLocked()
            true
        }

    private fun publishDownloadReadyFor(
        operation: ActiveOperation,
        state: ApkShareState.ReadyToDownload,
    ): Boolean = synchronized(operationLock) {
        if (activeOperation !== operation) return false
        operation.downloadReadyState = state
        mutableState.value = state
        scheduleNotificationLocked()
        true
    }

    private fun cancelActiveAndPublish(state: ApkShareState) {
        val detached = synchronized(operationLock) {
            val current = activeOperation
            activeOperation = null
            mutableState.value = state
            scheduleNotificationLocked()
            current
        }
        closeAsync(detached)
    }

    private fun attachHotspot(operation: ActiveOperation, hotspot: HotspotController): Boolean =
        synchronized(operationLock) {
            if (activeOperation !== operation) return false
            operation.hotspot = hotspot
            true
        }

    private fun releaseHotspot(operation: ActiveOperation) {
        val hotspot = synchronized(operationLock) {
            if (activeOperation !== operation) return
            operation.hotspot.also { operation.hotspot = null }
        }
        closeAsync { hotspot?.close() }
    }

    private fun detachActive(): ActiveOperation? = synchronized(operationLock) {
        activeOperation.also { activeOperation = null }
    }

    private fun closeAsync(operation: ActiveOperation?) {
        operation ?: return
        closeAsync(operation::close)
    }

    private fun closeAsync(action: () -> Unit) {
        try {
            cleanupExecutor.execute { runCatching(action) }
        } catch (_: RejectedExecutionException) {
            // onDestroy may race a final callback. Preserve cleanup even after the
            // executor has stopped accepting work.
            runCatching(action)
        }
    }

    private fun hasPermission(requirement: HotspotRequirement): Boolean = when (requirement) {
        HotspotRequirement.MANUAL -> true
        HotspotRequirement.FINE_LOCATION -> ContextCompat.checkSelfPermission(
            this,
            Manifest.permission.ACCESS_FINE_LOCATION,
        ) == PackageManager.PERMISSION_GRANTED

        HotspotRequirement.NEARBY_WIFI -> ContextCompat.checkSelfPermission(
            this,
            NEARBY_WIFI_PERMISSION,
        ) == PackageManager.PERMISSION_GRANTED
    }

    private fun scheduleNotificationLocked() {
        if (notificationGate.request()) {
            mainHandler.post(notificationRunnable)
        }
    }

    companion object {
        const val ACTION_CANCEL = "com.example.transfer.apkshare.action.CANCEL"
        const val ACTION_STOP_IF_IDLE = "com.example.transfer.apkshare.action.STOP_IF_IDLE"
        private const val APK_SHARE_CACHE_DIRECTORY = "apk-share"
        private const val COMPLETION_CLEANUP_DELAY_MILLIS = 300L
        private const val NEARBY_WIFI_PERMISSION = "android.permission.NEARBY_WIFI_DEVICES"
    }
}
