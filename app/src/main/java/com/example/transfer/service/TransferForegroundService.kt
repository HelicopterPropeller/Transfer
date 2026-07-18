package com.example.transfer.service

import android.app.Service
import android.content.Intent
import android.net.wifi.WifiManager
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import android.util.Log
import androidx.core.content.edit
import androidx.core.net.toUri
import com.example.transfer.batch.OutgoingBatchCoordinator
import com.example.transfer.batch.OutgoingBatchItem
import com.example.transfer.batch.OutgoingBatchState
import com.example.transfer.batch.RoomOutgoingBatchStore
import com.example.transfer.discovery.DiscoveryManager
import com.example.transfer.discovery.DiscoveredDevice
import com.example.transfer.discovery.PeerOrigin
import com.example.transfer.history.HistoryPeer
import com.example.transfer.history.IncomingHistoryRecorder
import com.example.transfer.history.OutgoingHistoryRecorder
import com.example.transfer.history.TransferHistoryDatabase
import com.example.transfer.history.TransferHistoryRepository
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.pairing.PairingClient
import com.example.transfer.pairing.PairingPayload
import com.example.transfer.pairing.PairingPayloadCodec
import com.example.transfer.pairing.PairingResponse
import com.example.transfer.pairing.PairingStatus
import com.example.transfer.pairing.PairingTokenManager
import com.example.transfer.pairing.WifiLanAddressProvider
import com.example.transfer.resume.ResumeCoordinator
import com.example.transfer.resume.PreparedTransfer
import com.example.transfer.resume.ResumeCleanup
import com.example.transfer.resume.RoomResumeStore
import com.example.transfer.storage.DownloadStorage
import com.example.transfer.transfer.FileTransferClient
import com.example.transfer.transfer.BatchFailure
import com.example.transfer.transfer.BatchTransferItem
import com.example.transfer.transfer.FileTransferServer
import com.example.transfer.transfer.SendFileSource
import com.example.transfer.transfer.SendFileMetadata
import com.example.transfer.transfer.TransferBatchRunner
import com.example.transfer.transfer.TransferPauseController
import com.example.transfer.transfer.TransferPauseState
import com.example.transfer.transfer.formatBatchCompletion
import com.example.transfer.ui.SelectedFile
import com.example.transfer.ui.SelectedFileResolver
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.UUID
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean

private data class PreparedOutgoingFile(
    val device: DiscoveredDevice,
    val prepared: PreparedTransfer,
    val batchItem: OutgoingBatchItem,
    val preparationFailures: List<BatchFailure>
)

class TransferForegroundService : Service() {
    inner class LocalBinder : Binder(), TransferServiceApi {
        override val state: StateFlow<ServiceTransferState>
            get() = this@TransferForegroundService.state
        override fun send(deviceId: String, files: List<SelectedFile>): Boolean =
            this@TransferForegroundService.send(deviceId, files)
        override fun confirmResume(promptId: Long, choice: ResumeChoice): Boolean =
            this@TransferForegroundService.confirmResume(promptId, choice)
        override fun pause(): Boolean = this@TransferForegroundService.pause()
        override fun resume(): Boolean = this@TransferForegroundService.resume()
        override fun cancelOutgoing(): Boolean = this@TransferForegroundService.cancelOutgoing()
        override fun createPairingOffer(): Boolean =
            this@TransferForegroundService.createPairingOffer()
        override fun dismissPairingOffer(rawPayload: String): Boolean =
            this@TransferForegroundService.dismissPairingOffer(rawPayload)
        override fun connectQr(rawPayload: String): Boolean =
            this@TransferForegroundService.connectQr(rawPayload)
        override fun acknowledgeQrPeer(deviceId: String): Boolean =
            this@TransferForegroundService.acknowledgeQrPeer(deviceId)
    }

    private val binder = LocalBinder()
    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val resumePreflight = ResumePreflight<PreparedOutgoingFile>()
    private val shutdownCoordinator = ServiceShutdownCoordinator()
    private val terminationGate = ServiceTerminationGate()
    private val mutableState = MutableStateFlow(ServiceTransferState())
    val state: StateFlow<ServiceTransferState> = mutableState.asStateFlow()
    private lateinit var notificationFactory: TransferNotificationFactory
    private lateinit var resourceGuard: TransferResourceGuard
    private lateinit var transferLifecycle: TransferServiceLifecycleCoordinator
    private lateinit var incomingAttempts: IncomingTransferAttemptTracker
    private lateinit var discovery: DiscoveryManager
    private lateinit var server: FileTransferServer
    private lateinit var historyRepository: TransferHistoryRepository
    private lateinit var outgoingHistoryRecorder: OutgoingHistoryRecorder
    private lateinit var historyStartupGate: HistoryStartupGate
    private lateinit var resumeCoordinator: ResumeCoordinator
    private lateinit var outgoingBatchCoordinator: OutgoingBatchCoordinator
    private lateinit var resumeStartupGate: ResumeStartupGate
    private lateinit var resumeMaintenance: ResumeMaintenance
    private lateinit var localDeviceId: String
    private lateinit var localDeviceName: String
    private lateinit var lanAddressProvider: WifiLanAddressProvider
    private val client = FileTransferClient()
    private val pairingClient = PairingClient()
    private val pairingTokens = PairingTokenManager()
    private val pairingConnecting = AtomicBoolean(false)
    @Volatile private var activePauseController: TransferPauseController? = null
    @Volatile private var activeBatchJob: Job? = null
    @Volatile private var activePreflightJob: Job? = null
    @Volatile private var incomingStartupJob: Job? = null

    override fun onCreate() {
        super.onCreate()
        val database = TransferHistoryDatabase.getInstance(this)
        historyRepository = TransferHistoryRepository(database.transferHistoryDao())
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
        transferLifecycle = TransferServiceLifecycleCoordinator(
            TransferLaneGate(resourceGuard::acquire, resourceGuard::release)
        )
        incomingAttempts = IncomingTransferAttemptTracker(
            acquire = transferLifecycle::beginIncoming,
            release = { transferLifecycle.endIncoming() },
        )
        localDeviceId = getSharedPreferences("transfer", 0).let { preferences ->
            preferences.getString("device_id", null) ?: UUID.randomUUID().toString().also {
                preferences.edit { putString("device_id", it) }
            }
        }
        localDeviceName = listOf(Build.MANUFACTURER, Build.MODEL).filter(String::isNotBlank)
            .joinToString(" ").ifBlank { "Android 设备" }
        lanAddressProvider = WifiLanAddressProvider(this)
        discovery = DiscoveryManager(this, localDeviceId, localDeviceName)
        val downloadStorage = DownloadStorage(this)
        val resumeStore = RoomResumeStore(database.resumeDao())
        resumeCoordinator = ResumeCoordinator(resumeStore, downloadStorage)
        outgoingBatchCoordinator = OutgoingBatchCoordinator(
            RoomOutgoingBatchStore(database.outgoingBatchDao())
        )
        val resumePreferences = getSharedPreferences("resume", 0)
        resumeStartupGate = ResumeStartupGate(
            scope = serviceScope,
            recover = {
                resumeCoordinator.recoverInterruptedState(Long.MAX_VALUE)
                outgoingBatchCoordinator.recoverInterrupted()
            }
        )
        resumeMaintenance = ResumeMaintenance(serviceScope) {
            resumeStartupGate.awaitReady()
            ResumeCleanup(
                store = resumeStore,
                files = downloadStorage,
                clock = System::currentTimeMillis,
                lastRun = { resumePreferences.getLong("cleanup_last_run", 0L) },
                saveLastRun = { value ->
                    resumePreferences.edit { putLong("cleanup_last_run", value) }
                }
            ).runIfDue()
        }
        server = FileTransferServer(
            resumeCoordinator = resumeCoordinator,
            pairingRequestHandler = { request ->
                val port = server.listeningPort()
                if (port != null && pairingTokens.consume(request.token)) {
                    publish { current -> current.copy(pairingOffer = null) }
                    PairingResponse.accepted(localDeviceId, localDeviceName, port)
                } else {
                    PairingResponse(PairingStatus.REJECTED)
                }
            },
            onTransferAttemptStart = ::beginIncomingAttempt,
            onTransferAttemptEnd = ::endIncomingAttempt,
            history = IncomingHistoryRecorder(historyRepository) { address ->
                val device = state.value.devices.firstOrNull {
                    it.address.hostAddress == address
                }
                HistoryPeer(
                    id = device?.id,
                    name = device?.name,
                    address = address
                )
            }
        )
        incomingStartupJob = resumeStartupGate.launchWhenReady(
            onFailure = { error ->
                runCatching {
                    publish { current ->
                        current.copy(
                            serviceMessage = "断点恢复初始化失败",
                            incomingTransfer = ServiceTransfer(
                                "接收",
                                "",
                                0,
                                error.message ?: "无法恢复未完成传输",
                                false
                            )
                        )
                    }
                }
            }
        ) {
            refreshRecoverableBatch()
            historyStartupGate.awaitReady()
            server.start(
                serviceScope,
                { publish { it.copy(serviceMessage = "后台接收服务运行中 · TCP $it") } },
                { attemptId, name, progress ->
                    publish { current ->
                        if (!incomingAttempts.isCurrent(attemptId) ||
                            current.incomingTransfer?.incomingAttemptId != attemptId
                        ) current else current.copy(
                            incomingTransfer = ServiceTransfer(
                                "接收", name, progress, "正在接收", true,
                                incomingAttemptId = attemptId
                            )
                        )
                    }
                },
                { attemptId, name ->
                    finishIncomingAttempt(attemptId) { current ->
                        current.copy(
                            incomingTransfer = ServiceTransfer(
                                "接收", name, 100, "接收完成", false,
                                incomingAttemptId = attemptId
                            )
                        )
                    }
                },
                { attemptId, message ->
                    if (attemptId == null) {
                        publish { it.copy(serviceMessage = message) }
                    } else {
                        finishIncomingAttempt(attemptId) { current ->
                            val transfer = current.incomingTransfer
                            current.copy(
                                incomingTransfer = ServiceTransfer(
                                    "接收",
                                    transfer?.fileName.orEmpty(),
                                    transfer?.progress ?: 0,
                                    "$message，可稍后续传",
                                    false,
                                    incomingAttemptId = attemptId
                                )
                            )
                        }
                    }
                }
            )
        }
        discovery.start(
            serviceScope,
            { devices -> publish { it.copy(devices = devices) } },
            { message -> publish { it.copy(serviceMessage = message) } }
        )
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_PAUSE) pause()
        if (intent?.action == ACTION_RESUME) resume()
        if (intent?.action == ACTION_CANCEL) cancelOutgoing()
        if (intent?.action == ACTION_STOP) {
            shutdown()
            stopForeground(STOP_FOREGROUND_REMOVE)
            stopSelf()
            return START_NOT_STICKY
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder = binder

    fun createPairingOffer(): Boolean {
        var created = false
        terminationGate.runIfOpen {
            val port = server.listeningPort()
            val address = lanAddressProvider.currentPrivateIpv4()
            if (port == null || address == null) {
                publishNow { current ->
                    current.copy(serviceMessage = "QR connection requires an active private Wi-Fi address")
                }
                return@runIfOpen
            }
            val token = pairingTokens.issue()
            val payload = PairingPayload(
                version = 4,
                deviceId = localDeviceId,
                deviceName = localDeviceName,
                ip = address.hostAddress.orEmpty(),
                port = port,
                token = token.value,
                expiresAt = token.expiresAt
            )
            val raw = runCatching { PairingPayloadCodec.encode(payload) }.getOrElse {
                pairingTokens.invalidate()
                publishNow { current -> current.copy(serviceMessage = "Unable to create pairing QR") }
                return@runIfOpen
            }
            publishNow { current ->
                current.copy(pairingOffer = PairingOfferUi(raw, payload.ip, token.expiresAt))
            }
            created = true
        }
        return created
    }

    fun dismissPairingOffer(rawPayload: String): Boolean {
        var dismissed = false
        terminationGate.runIfOpen {
            if (state.value.pairingOffer?.rawPayload != rawPayload) return@runIfOpen
            pairingTokens.invalidate()
            publishNow { current -> current.copy(pairingOffer = null) }
            dismissed = true
        }
        return dismissed
    }

    fun connectQr(rawPayload: String): Boolean {
        val payload = runCatching {
            PairingPayloadCodec.decode(rawPayload, System.currentTimeMillis())
        }.getOrElse {
            publish { current -> current.copy(serviceMessage = "Invalid or expired connection QR") }
            return false
        }
        if (payload.deviceId == localDeviceId || !pairingConnecting.compareAndSet(false, true)) {
            publish { current -> current.copy(serviceMessage = "Unable to connect to this QR") }
            return false
        }
        val started = terminationGate.runIfOpen {
            serviceScope.launch {
                try {
                    val peer = pairingClient.connect(payload, localDeviceId, localDeviceName)
                    discovery.addQrPeer(
                        DiscoveredDevice(
                            id = peer.id,
                            name = peer.name,
                            address = peer.address,
                            port = peer.port,
                            lastSeenMillis = System.currentTimeMillis(),
                            origin = PeerOrigin.QR
                        )
                    )
                    publish { current ->
                        current.copy(
                            preferredQrPeerId = peer.id,
                            serviceMessage = "Connected to ${peer.name} by QR"
                        )
                    }
                } catch (error: Exception) {
                    publish { current ->
                        current.copy(serviceMessage = "QR connection failed: ${error.message.orEmpty()}")
                    }
                } finally {
                    pairingConnecting.set(false)
                }
            }
        }
        if (!started) pairingConnecting.set(false)
        return started
    }

    fun acknowledgeQrPeer(deviceId: String): Boolean {
        var acknowledged = false
        terminationGate.runIfOpen {
            if (state.value.preferredQrPeerId != deviceId) return@runIfOpen
            publishNow { current -> current.copy(preferredQrPeerId = null) }
            acknowledged = true
        }
        return acknowledged
    }

    fun send(deviceId: String, files: List<SelectedFile>): Boolean {
        if (files.isEmpty() || checkedBatchLength(files.map { it.size }) == null) return false
        var accepted = false
        terminationGate.runIfOpen gate@{
            val device = state.value.devices.firstOrNull { it.id == deviceId } ?: return@gate
            val reservation = resumePreflight.reserve {
                transferLifecycle.isOutgoingActive()
            } ?: return@gate
            lateinit var job: Job
            job = serviceScope.launch(start = CoroutineStart.LAZY) {
                var batchId: String? = null
                try {
                    historyStartupGate.awaitReady()
                    resumeStartupGate.awaitReady()
                    val batch = outgoingBatchCoordinator.acquire(device.id, files)
                    batchId = batch.batch.batchId
                    refreshRecoverableBatch()
                    val sourceResolver = SelectedFileResolver(this@TransferForegroundService)
                    val candidates = batch.pendingItems.map { item ->
                        item to createBatchSource(item, sourceResolver)
                    }
                    val preflight = durableResumePreflight(
                        items = candidates,
                        prepare = { (_, source) ->
                            resumeCoordinator.prepareOutgoing(source, device.id, localDeviceId)
                        },
                        persistPrepared = { (item, _), transfer ->
                            check(outgoingBatchCoordinator.saveTransferId(
                                item, transfer.offer.transferId
                            )) { "无法保存批次文件断点标识" }
                        },
                        query = { transfer ->
                            client.queryResume(device.address, device.port, transfer.offer)
                        }
                    )
                    val preparationFailures = preflight.preparationFailures.map { failure ->
                        BatchFailure(
                            fileName = failure.item.second.displayName,
                            message = failure.error.message ?: "无法读取所选文件"
                        )
                    }
                    val prepared = preflight.ready.map { ready ->
                        ResumePreflightFile(
                            value = PreparedOutgoingFile(
                                device = device,
                                prepared = ready.prepared,
                                batchItem = ready.item.first,
                                preparationFailures = preparationFailures
                            ),
                            fileName = ready.item.second.displayName,
                            status = ready.remote
                        )
                    }
                    if (prepared.isEmpty()) {
                        outgoingBatchCoordinator.markBatch(
                            batch.batch.batchId, OutgoingBatchState.INTERRUPTED
                        )
                        refreshRecoverableBatch()
                        resumePreflight.cancelReservation(reservation)
                        publish { current ->
                            current.withInactiveBatchFailure("没有可读取的待发送文件")
                        }
                        return@launch
                    }
                    when (val result = resumePreflight.finish(
                        reservation, prepared, acquireBusy = ::beginOutgoing
                    )) {
                        is ResumePreflightResult.Ready -> {
                            if (!startPreparedOutgoing(result.files, alreadyHeld = true)) {
                                interruptOutgoingBatch(batch.batch.batchId)
                                publishBusyRace()
                            }
                        }
                        is ResumePreflightResult.Waiting -> publishResumePromptSafely(
                            token = reservation,
                            prompt = result.prompt,
                            cancelPending = resumePreflight::cancelReservation,
                            publishPrompt = ::publishResumePromptStrict,
                            clearPublicPrompt = { failedToken ->
                                mutableState.update { current ->
                                    if (current.resumePrompt?.id == failedToken) {
                                        current.copy(resumePrompt = null)
                                    } else {
                                        current
                                    }
                                }
                            },
                            publishError = { failedToken, error ->
                                publish { current ->
                                    current.withResumePromptPublicationFailure(
                                        failedToken,
                                        "续传提示发布失败：${error.message ?: "未知错误"}"
                                    )
                                }
                            }
                        )
                        ResumePreflightResult.Busy -> {
                            interruptOutgoingBatch(batch.batch.batchId)
                            publishBusyRace()
                        }
                        ResumePreflightResult.Ignored ->
                            interruptOutgoingBatch(batch.batch.batchId)
                    }
                } catch (exception: CancellationException) {
                    resumePreflight.cancelReservation(reservation)
                    batchId?.let { interruptOutgoingBatch(it) }
                    throw exception
                } catch (exception: Exception) {
                    resumePreflight.cancelReservation(reservation)
                    batchId?.let { interruptOutgoingBatch(it) }
                    publish { current ->
                        val message = "发送预检失败：${exception.message ?: "未知错误"}"
                        if (current.outgoingTransfer?.active == true) {
                            current.copy(serviceMessage = message)
                        } else {
                            current.withInactiveBatchFailure(message)
                        }
                    }
                } finally {
                    if (activePreflightJob === job) activePreflightJob = null
                }
            }
            activePreflightJob = job
            accepted = job.start()
            if (!accepted) {
                resumePreflight.cancelReservation(reservation)
                if (activePreflightJob === job) activePreflightJob = null
            }
        }
        return accepted
    }

    fun confirmResume(promptId: Long, choice: ResumeChoice): Boolean {
        var handled = false
        terminationGate.runIfOpen {
            when (val confirmation = resumePreflight.confirm(
                promptId, choice, acquireBusy = ::beginOutgoing
            )) {
                ResumeConfirmation.Ignored -> Unit
                ResumeConfirmation.Cancelled -> {
                    handled = true
                    publish { it.copy(resumePrompt = null) }
                }
                is ResumeConfirmation.Ready -> {
                    handled = true
                    publish { it.copy(resumePrompt = null) }
                    if (!startPreparedOutgoing(confirmation.files, alreadyHeld = true)) {
                        publishBusyRace()
                    }
                }
                ResumeConfirmation.Busy -> {
                    handled = true
                    publishBusyRace()
                }
            }
        }
        return handled
    }

    private fun startPreparedOutgoing(
        files: List<ResumeSelectedFile<PreparedOutgoingFile>>,
        alreadyHeld: Boolean = false
    ): Boolean {
        if (files.isEmpty()) {
            if (alreadyHeld) endOutgoing()
            return false
        }
        if (!alreadyHeld && !beginOutgoing()) return false
        val device = files.first().value.device
        val batchId = files.first().value.batchItem.batchId
        val sources = files.map { it.value.prepared.source }
        val controller = TransferPauseController()
        activePauseController = controller
        publish { current ->
            current.copy(
                resumePrompt = null,
                outgoingTransfer = ServiceTransfer(
                    direction = "发送",
                    fileName = sources.first().displayName,
                    progress = 0,
                    message = "正在连接 ${device.name}",
                    active = true,
                    fileIndex = 1,
                    fileCount = sources.size,
                    batchProgress = 0,
                    pauseState = controller.state
                )
            )
        }
        lateinit var job: Job
        val cleanupStarted = AtomicBoolean(false)
        val cleanup = {
            if (cleanupStarted.compareAndSet(false, true)) {
                if (activePauseController === controller) activePauseController = null
                if (activeBatchJob === job) activeBatchJob = null
                endOutgoing()
            }
        }
        job = serviceScope.launch(start = CoroutineStart.LAZY) {
            try {
                check(outgoingBatchCoordinator.markBatch(
                    batchId, OutgoingBatchState.ACTIVE
                )) { "无法更新批次状态" }
                val onPauseState: (TransferPauseState) -> Unit = { publishPauseState(controller) }
                val runner = TransferBatchRunner(controller) { source, onProgress ->
                    val selected = files.first { it.value.prepared.source === source }
                    val prepared = selected.value.prepared
                    val batchItem = selected.value.batchItem
                    check(outgoingBatchCoordinator.markItemActive(batchItem)) {
                        "无法更新批次文件状态"
                    }
                    val result = try {
                        outgoingHistoryRecorder.send(
                            source = source,
                            peer = HistoryPeer(
                                id = device.id,
                                name = device.name,
                                address = device.address.hostAddress
                            ),
                            controller = controller,
                        ) {
                            val networkResult = client.sendPrepared(
                                device.address,
                                device.port,
                                prepared.copy(resumeStatus = selected.status),
                                selected.mode,
                                selected.status,
                                controller,
                                onPauseState,
                                onProgress
                            )
                            val durableResult = if (networkResult.isSuccess) {
                                if (outgoingBatchCoordinator.markItemSucceeded(batchItem)) {
                                    networkResult
                                } else {
                                    Result.failure(IllegalStateException(
                                        "接收方已完成，但无法保存批次完成状态"
                                    ))
                                }
                            } else {
                                networkResult
                            }
                            finalizeOutgoingNetworkResult(
                                durableResult,
                                onCleanupFailure = { error ->
                                    Log.w(
                                        LOG_TAG,
                                        "Unable to delete completed outgoing resume link",
                                        error
                                    )
                                }
                            ) { resumeCoordinator.completeOutgoing(prepared.offer.transferId) }
                        }
                    } catch (cancelled: CancellationException) {
                        markOutgoingItemPendingBestEffort(batchItem)
                        throw cancelled
                    }
                    if (result.isFailure) markOutgoingItemPendingBestEffort(batchItem)
                    result
                }
                var lastProgress: com.example.transfer.transfer.BatchTransferProgress? = null
                val result = runner.runItems(
                    files.map { selected ->
                        BatchTransferItem(
                            source = selected.value.prepared.source,
                            initialConfirmedBytes = if (selected.mode == TransferStartMode.RESUME) {
                                selected.status.confirmedBytes
                            } else {
                                0L
                            }
                        )
                    },
                    onProgress = { progress ->
                        lastProgress = progress
                        publishOutgoingProgress(controller, progress)
                    },
                    onPauseState = onPauseState
                )
                val combinedResult = result.copy(
                    failures = files.first().value.preparationFailures + result.failures
                )
                finishOutgoingBatchAttempt(batchId)
                publish { current ->
                    if (activePauseController !== controller) current else {
                        val terminal = lastProgress
                        current.copy(outgoingTransfer = ServiceTransfer(
                            direction = "发送",
                            fileName = terminal?.fileName ?: sources.last().displayName,
                            progress = terminal?.fileProgress ?: 0,
                            message = formatBatchCompletion(combinedResult),
                            active = false,
                            fileIndex = terminal?.fileIndex ?: sources.size,
                            fileCount = sources.size,
                            batchProgress = terminal?.batchProgress ?: 0,
                            pauseState = controller.state
                        ))
                    }
                }
            } catch (exception: CancellationException) {
                interruptOutgoingBatch(batchId)
                publish { current ->
                    if (activePauseController !== controller) current else {
                        current.withCancelledOutgoing("传输已取消，可稍后继续")
                    }
                }
                throw exception
            } catch (exception: Exception) {
                interruptOutgoingBatch(batchId)
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
        val started = job.start()
        if (!started) cleanup()
        return started
    }

    private fun publishOutgoingProgress(
        controller: TransferPauseController,
        progress: com.example.transfer.transfer.BatchTransferProgress
    ) {
        publish { current ->
            if (activePauseController !== controller || current.outgoingTransfer?.active != true) {
                current
            } else {
                val pauseState = controller.state
                current.copy(outgoingTransfer = ServiceTransfer(
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
    }

    private fun publishBusyRace() {
        publish { current ->
            current.copy(
                resumePrompt = null,
                serviceMessage = "已有传输任务，请稍后重试"
            )
        }
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

    fun cancelOutgoing(): Boolean {
        val controller = activePauseController ?: return false
        val job = activeBatchJob ?: return false
        if (controller.state == TransferPauseState.CANCELLED) return false
        controller.cancel()
        publishPauseState(controller)
        client.cancelActive()
        job.cancel(CancellationException("Outgoing transfer cancelled"))
        return true
    }

    private fun beginOutgoing(): Boolean = transferLifecycle.beginOutgoing()

    private fun endOutgoing() {
        transferLifecycle.endOutgoing()
    }

    private fun beginIncomingAttempt(attemptId: Long): Boolean {
        if (!incomingAttempts.begin(attemptId)) return false
        publish { current ->
            current.copy(
                incomingTransfer = ServiceTransfer(
                    "接收", "", 0, "正在连接", true,
                    incomingAttemptId = attemptId
                )
            )
        }
        return true
    }

    private fun endIncomingAttempt(attemptId: Long) {
        finishIncomingAttempt(attemptId) { current ->
            val transfer = current.incomingTransfer
            if (transfer?.incomingAttemptId == attemptId && transfer.active) {
                current.copy(
                    incomingTransfer = transfer.copy(
                        active = false,
                        message = "连接已中断，可稍后续传"
                    )
                )
            } else {
                current
            }
        }
    }

    private fun finishIncomingAttempt(
        attemptId: Long,
        terminal: (ServiceTransferState) -> ServiceTransferState
    ) {
        incomingAttempts.finish(attemptId) {
            publish { current ->
                if (current.incomingTransfer?.incomingAttemptId == attemptId) terminal(current) else current
            }
        }
    }

    private fun createBatchSource(
        item: OutgoingBatchItem,
        sourceResolver: SelectedFileResolver
    ): SendFileSource {
        val uri = item.sourceUri.toUri()
        return SendFileSource(
            displayName = item.displayName,
            mimeType = item.mimeType,
            length = item.fileSize,
            sourceUri = item.sourceUri,
            lastModified = item.lastModified,
            openStream = {
                contentResolver.openInputStream(uri) ?: error("无法读取所选文件")
            },
            metadataProvider = {
                val current = sourceResolver.stat(uri) ?: error("无法读取所选文件")
                SendFileMetadata(current.size, current.lastModified)
            }
        )
    }

    private suspend fun markOutgoingItemPendingBestEffort(item: OutgoingBatchItem) {
        withContext(NonCancellable) {
            runCatching { outgoingBatchCoordinator.markItemPending(item) }
                .onFailure { error ->
                    Log.w(LOG_TAG, "Unable to restore outgoing batch item to pending", error)
                }
        }
    }

    private suspend fun interruptOutgoingBatch(batchId: String) {
        withContext(NonCancellable) {
            runCatching {
                outgoingBatchCoordinator.markBatch(batchId, OutgoingBatchState.INTERRUPTED)
                outgoingBatchCoordinator.recoverInterrupted()
                refreshRecoverableBatch()
            }.onFailure { error ->
                Log.w(LOG_TAG, "Unable to persist interrupted outgoing batch", error)
            }
        }
    }

    private suspend fun finishOutgoingBatchAttempt(batchId: String) {
        withContext(NonCancellable) {
            val snapshot = outgoingBatchCoordinator.find(batchId)
            if (snapshot != null && snapshot.pendingItems.isEmpty()) {
                outgoingBatchCoordinator.delete(batchId)
            } else if (snapshot != null) {
                outgoingBatchCoordinator.markBatch(batchId, OutgoingBatchState.INTERRUPTED)
                outgoingBatchCoordinator.recoverInterrupted()
            }
            refreshRecoverableBatch()
        }
    }

    private suspend fun refreshRecoverableBatch() {
        val recoverable = outgoingBatchCoordinator.latestRecoverable()?.let { snapshot ->
            RecoverableOutgoingBatch(
                batchId = snapshot.batch.batchId,
                peerDeviceId = snapshot.batch.peerDeviceId,
                files = snapshot.pendingItems.map { item ->
                    SelectedFile(
                        uri = item.sourceUri,
                        displayName = item.displayName,
                        mimeType = item.mimeType,
                        size = item.fileSize,
                        lastModified = item.lastModified
                    )
                }
            )
        }
        publish { current -> current.copy(recoverableBatch = recoverable) }
    }

    private fun publish(transform: (ServiceTransferState) -> ServiceTransferState) {
        terminationGate.runIfOpen {
            publishNow(transform)
        }
    }

    private fun publishNow(transform: (ServiceTransferState) -> ServiceTransferState) {
        mutableState.update(transform)
        runCatching {
            startForeground(
                TransferNotificationFactory.NOTIFICATION_ID,
                notificationFactory.build(TransferNotificationModel.from(mutableState.value))
            )
        }
    }

    private fun publishResumePromptStrict(prompt: ResumePrompt) {
        val published = terminationGate.runIfOpen {
            mutableState.update { current -> current.copy(resumePrompt = prompt) }
            startForeground(
                TransferNotificationFactory.NOTIFICATION_ID,
                notificationFactory.build(TransferNotificationModel.from(mutableState.value))
            )
        }
        if (!published) throw IllegalStateException("Service is stopping")
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
        pairingTokens.invalidate()
        shutdownCoordinator.shutdown(
            cancelOutgoing = {
                resumePreflight.clear()
                pairingClient.cancelActive()
                val jobSnapshots = listOf(
                    activePreflightJob,
                    activeBatchJob,
                    incomingStartupJob,
                )
                activePauseController?.cancel()
                client.cancelActive()
                ServiceShutdownJobs.cancelAndJoin(jobSnapshots)
                ServiceShutdownJobs.cancelAndJoin(listOf(activeBatchJob))
            },
            preventNewStarts = {
                if (::server.isInitialized) server.stop()
                if (::transferLifecycle.isInitialized) transferLifecycle.drain()
            },
            stopResources = {
                if (::resumeMaintenance.isInitialized) resumeMaintenance.cancel()
                if (::discovery.isInitialized) discovery.stop()
                serviceScope.cancel()
            }
        )
    }

    override fun onDestroy() {
        shutdown()
        super.onDestroy()
    }

    override fun onTimeout(startId: Int, fgsType: Int) {
        try {
            terminationGate.closeWithAction {
                publishNow {
                    it.copy(
                        serviceMessage = "后台服务达到系统运行时限",
                        outgoingTransfer = it.outgoingTransfer?.copy(
                            message = "后台服务达到系统运行时限",
                            active = false
                        ),
                        incomingTransfer = it.incomingTransfer?.copy(
                            message = "后台服务达到系统运行时限",
                            active = false
                        )
                    )
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
        const val ACTION_CANCEL = "com.example.transfer.action.CANCEL"
        const val ACTION_STOP = "com.example.transfer.action.STOP"
        private const val MAX_LOCK_MILLIS = 12L * 60 * 60 * 1000
        private const val LOG_TAG = "TransferService"
    }
}
