package com.example.transfer

import android.Manifest
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.DialogInterface
import android.content.ServiceConnection
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import android.os.CountDownTimer
import android.text.format.Formatter
import android.view.LayoutInflater
import android.view.View
import android.widget.LinearLayout
import android.widget.ImageView
import android.widget.ProgressBar
import android.widget.TextView
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.net.toUri
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.doOnLayout
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.example.transfer.history.HistoryActivity
import com.example.transfer.history.HistoryRetryContract
import com.example.transfer.pairing.QrBitmapEncoder
import com.example.transfer.pairing.QrDisplaySizer
import com.example.transfer.pairing.QrScanConfiguration
import com.example.transfer.service.PairingOfferUi
import com.example.transfer.service.TransferForegroundService
import com.example.transfer.service.ResumeChoice
import com.example.transfer.service.TransferServiceApi
import com.example.transfer.ui.LatestSelectionRequest
import com.example.transfer.ui.ResumePromptDisplayTracker
import com.example.transfer.ui.SelectedFile
import com.example.transfer.ui.SelectedFileResolver
import com.example.transfer.ui.TransferStatus
import com.example.transfer.ui.TransferUiState
import com.google.android.material.button.MaterialButton
import com.google.android.material.card.MaterialCardView
import com.google.android.material.dialog.MaterialAlertDialogBuilder
import com.journeyapps.barcodescanner.ScanContract
import com.journeyapps.barcodescanner.ScanOptions
import androidx.appcompat.app.AlertDialog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class MainActivity : AppCompatActivity() {
    private val viewModel: TransferViewModel by viewModels()
    private val latestSelectionRequest = LatestSelectionRequest()
    private val resumePromptDisplayTracker = ResumePromptDisplayTracker()
    private val selectedFileResolver by lazy { SelectedFileResolver(this) }
    private var serviceBound = false
    private val serviceConnection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, binder: IBinder?) {
            val service = binder as? TransferServiceApi ?: return
            serviceBound = true
            viewModel.attachService(service)
        }
        override fun onServiceDisconnected(name: ComponentName?) {
            serviceBound = false
            viewModel.detachService()
        }
    }

    private lateinit var deviceNameText: TextView
    private lateinit var serviceStatusText: TextView
    private lateinit var emptyDevicesText: TextView
    private lateinit var deviceContainer: LinearLayout
    private lateinit var selectedFileText: TextView
    private lateinit var noticeText: TextView
    private lateinit var sendButton: MaterialButton
    private lateinit var outgoingTransferCard: MaterialCardView
    private lateinit var outgoingTransferTitleText: TextView
    private lateinit var outgoingTransferFileText: TextView
    private lateinit var outgoingTransferBatchText: TextView
    private lateinit var outgoingTransferProgress: ProgressBar
    private lateinit var outgoingTransferMessageText: TextView
    private lateinit var outgoingPauseResumeButton: MaterialButton
    private lateinit var outgoingCancelTransferButton: MaterialButton
    private lateinit var incomingTransferCard: MaterialCardView
    private lateinit var incomingTransferTitleText: TextView
    private lateinit var incomingTransferFileText: TextView
    private lateinit var incomingTransferProgress: ProgressBar
    private lateinit var incomingTransferMessageText: TextView
    private var pairingDialog: AlertDialog? = null
    private var pairingDialogPayload: String? = null
    private var pairingCountdown: CountDownTimer? = null

    private val openDocuments = registerForActivityResult(ActivityResultContracts.OpenMultipleDocuments()) { uris ->
        val requestToken = latestSelectionRequest.nextTokenForSelection(uris.size)
            ?: return@registerForActivityResult
        lifecycleScope.launch {
            val (files, skipped) = withContext(Dispatchers.IO) {
                val readableFiles = ArrayList<SelectedFile>(uris.size)
                var skippedFiles = 0
                uris.forEach { uri ->
                    val file = selectedFileResolver.resolve(uri, persistPermission = true)
                    if (file == null) skippedFiles++ else readableFiles += file
                }
                readableFiles to skippedFiles
            }
            val notice = skipped.takeIf { it > 0 }?.let { getString(R.string.files_skipped, it) }
            latestSelectionRequest.completeIfLatest(
                requestToken,
                publish = { viewModel.selectFiles(files, notice) },
                consume = { HistoryRetryContract.clear(intent) }
            )
        }
    }

    private val requestStoragePermission = registerForActivityResult(ActivityResultContracts.RequestPermission()) {
        if (!it) viewModel.showMessage(getString(R.string.storage_permission_denied))
    }
    private val requestNotificationPermission = registerForActivityResult(ActivityResultContracts.RequestPermission()) {
        if (!it) viewModel.showMessage(getString(R.string.notification_permission_denied))
    }
    private val scanQr = registerForActivityResult(ScanContract()) { result ->
        result.contents?.let(viewModel::connectQr)
            ?: viewModel.showMessage(getString(R.string.qr_scan_cancelled))
    }
    private val requestCameraPermission = registerForActivityResult(ActivityResultContracts.RequestPermission()) {
        if (it) launchQrScanner()
        else viewModel.showMessage(getString(R.string.camera_permission_denied))
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContentView(R.layout.activity_main)
        bindViews()
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main)) { view, insets ->
            val bars = insets.getInsets(WindowInsetsCompat.Type.systemBars())
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            insets
        }
        deviceNameText.text = viewModel.deviceName
        findViewById<MaterialButton>(R.id.historyButton).setOnClickListener {
            startActivity(Intent(this, HistoryActivity::class.java))
        }
        findViewById<MaterialButton>(R.id.showQrButton).setOnClickListener {
            viewModel.createPairingOffer()
        }
        findViewById<MaterialButton>(R.id.scanQrButton).setOnClickListener {
            requestQrScan()
        }
        restoreHistoryRetry(intent)
        findViewById<MaterialButton>(R.id.selectFileButton).setOnClickListener { openDocuments.launch(arrayOf("*/*")) }
        sendButton.setOnClickListener { viewModel.sendSelected() }
        outgoingPauseResumeButton.setOnClickListener { viewModel.togglePause() }
        outgoingCancelTransferButton.setOnClickListener { confirmCancelTransfer() }
        val serviceIntent = Intent(this, TransferForegroundService::class.java)
            .setAction(TransferForegroundService.ACTION_START)
        ContextCompat.startForegroundService(this, serviceIntent)
        requestLegacyStoragePermissionIfNeeded()
        requestNotificationPermissionIfNeeded()
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) { viewModel.state.collect(::render) }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        restoreHistoryRetry(intent)
    }

    override fun onStart() {
        super.onStart()
        serviceBound = bindService(
            Intent(this, TransferForegroundService::class.java),
            serviceConnection,
            Context.BIND_AUTO_CREATE
        )
    }

    override fun onStop() {
        pairingDialog?.dismiss()
        viewModel.detachService()
        if (serviceBound) {
            unbindService(serviceConnection)
            serviceBound = false
        }
        super.onStop()
    }

    private fun bindViews() {
        deviceNameText = findViewById(R.id.deviceNameText)
        serviceStatusText = findViewById(R.id.serviceStatusText)
        emptyDevicesText = findViewById(R.id.emptyDevicesText)
        deviceContainer = findViewById(R.id.deviceContainer)
        selectedFileText = findViewById(R.id.selectedFileText)
        noticeText = findViewById(R.id.noticeText)
        sendButton = findViewById(R.id.sendButton)
        outgoingTransferCard = findViewById(R.id.outgoingTransferCard)
        outgoingTransferTitleText = findViewById(R.id.outgoingTransferTitleText)
        outgoingTransferFileText = findViewById(R.id.outgoingTransferFileText)
        outgoingTransferBatchText = findViewById(R.id.outgoingTransferBatchText)
        outgoingTransferProgress = findViewById(R.id.outgoingTransferProgress)
        outgoingTransferMessageText = findViewById(R.id.outgoingTransferMessageText)
        outgoingPauseResumeButton = findViewById(R.id.outgoingPauseResumeButton)
        outgoingCancelTransferButton = findViewById(R.id.outgoingCancelTransferButton)
        incomingTransferCard = findViewById(R.id.incomingTransferCard)
        incomingTransferTitleText = findViewById(R.id.incomingTransferTitleText)
        incomingTransferFileText = findViewById(R.id.incomingTransferFileText)
        incomingTransferProgress = findViewById(R.id.incomingTransferProgress)
        incomingTransferMessageText = findViewById(R.id.incomingTransferMessageText)
    }

    private fun render(state: TransferUiState) {
        renderPairingOffer(state.pairingOffer)
        serviceStatusText.text = state.serviceStatus
        emptyDevicesText.visibility = if (state.devices.isEmpty()) View.VISIBLE else View.GONE
        deviceContainer.removeAllViews()
        state.devices.forEach { device ->
            val row = LayoutInflater.from(this).inflate(R.layout.item_device, deviceContainer, false) as TextView
            row.text = getString(R.string.device_row, device.name, device.address.hostAddress.orEmpty(), device.port)
            row.setBackgroundResource(if (device.id == state.selectedDeviceId) R.drawable.bg_device_selected else R.drawable.bg_device)
            row.setOnClickListener { viewModel.selectDevice(device.id) }
            deviceContainer.addView(row)
        }
        selectedFileText.text = if (state.selectedFiles.isEmpty()) {
            getString(R.string.no_file_selected)
        } else {
            val total = saturatingTotalSize(state.selectedFiles)
            val size = total?.let { Formatter.formatFileSize(this, it) } ?: getString(R.string.unknown_size)
            getString(R.string.selected_files, state.selectedFiles.size, size)
        }
        noticeText.text = state.notice.orEmpty()
        noticeText.visibility = if (state.notice == null) View.GONE else View.VISIBLE
        sendButton.isEnabled = state.canSend
        renderOutgoingTransfer(state.outgoingTransfer, state)
        renderIncomingTransfer(state.incomingTransfer)
        state.resumePrompt?.let { prompt ->
            if (resumePromptDisplayTracker.shouldShow(prompt.id)) {
                MaterialAlertDialogBuilder(this)
                    .setTitle(R.string.resume_transfer_title)
                    .setMessage(resources.getQuantityString(
                        R.plurals.resume_transfer_message,
                        prompt.resumableFileNames.size,
                        prompt.resumableFileNames.size
                    ))
                    .setPositiveButton(R.string.resume_available) { _, _ ->
                        viewModel.confirmResume(prompt.id, ResumeChoice.RESUME_AVAILABLE)
                    }
                    .setNegativeButton(R.string.restart_all) { _, _ ->
                        viewModel.confirmResume(prompt.id, ResumeChoice.RESTART_ALL)
                    }
                    .setNeutralButton(android.R.string.cancel) { _, _ ->
                        viewModel.confirmResume(prompt.id, ResumeChoice.CANCEL)
                    }
                    .setOnCancelListener {
                        viewModel.confirmResume(prompt.id, ResumeChoice.CANCEL)
                    }
                    .show()
            }
        }
    }

    private fun renderOutgoingTransfer(transfer: TransferStatus?, state: TransferUiState) {
        outgoingTransferCard.visibility = if (transfer == null) View.GONE else View.VISIBLE
        if (transfer == null) return

        outgoingTransferTitleText.text = if (transfer.direction.isBlank()) {
            transfer.message
        } else {
            getString(R.string.transfer_title, transfer.direction)
        }
        outgoingTransferFileText.text = transfer.fileName
        outgoingTransferFileText.visibility = if (transfer.fileName.isBlank()) View.GONE else View.VISIBLE
        outgoingTransferBatchText.text = getString(
            R.string.transfer_batch,
            transfer.fileIndex,
            transfer.fileCount,
            transfer.batchProgress
        )
        outgoingTransferBatchText.visibility = if (transfer.active) View.VISIBLE else View.GONE
        outgoingTransferProgress.progress = transfer.progress
        outgoingTransferProgress.visibility = if (transfer.active) View.VISIBLE else View.GONE
        outgoingTransferMessageText.text = getString(
            R.string.transfer_message,
            transfer.progress,
            transfer.message
        )
        outgoingPauseResumeButton.visibility = if (state.canPause || state.canResume) {
            View.VISIBLE
        } else {
            View.GONE
        }
        outgoingPauseResumeButton.setText(if (state.canPause) R.string.pause else R.string.resume)
        outgoingCancelTransferButton.visibility = if (state.canCancel) View.VISIBLE else View.GONE
    }

    private fun renderIncomingTransfer(transfer: TransferStatus?) {
        incomingTransferCard.visibility = if (transfer == null) View.GONE else View.VISIBLE
        if (transfer == null) return

        incomingTransferTitleText.text = if (transfer.direction.isBlank()) {
            transfer.message
        } else {
            getString(R.string.transfer_title, transfer.direction)
        }
        incomingTransferFileText.text = transfer.fileName
        incomingTransferFileText.visibility = if (transfer.fileName.isBlank()) View.GONE else View.VISIBLE
        incomingTransferProgress.progress = transfer.progress
        incomingTransferProgress.visibility = if (transfer.active) View.VISIBLE else View.GONE
        incomingTransferMessageText.text = getString(
            R.string.transfer_message,
            transfer.progress,
            transfer.message
        )
    }

    private fun restoreHistoryRetry(retryIntent: Intent) {
        val request = HistoryRetryContract.read(retryIntent) ?: return
        val requestToken = latestSelectionRequest.nextToken()
        lifecycleScope.launch {
            val file = withContext(Dispatchers.IO) {
                selectedFileResolver.resolve(
                    request.sourceUri.toUri(),
                    persistPermission = false
                )
            }
            latestSelectionRequest.completeIfLatest(
                requestToken,
                publish = {
                    if (file == null) {
                        viewModel.showMessage(getString(R.string.history_source_unavailable))
                    } else {
                        viewModel.restoreHistoryFile(file, request.preferredPeerId)
                    }
                },
                consume = { HistoryRetryContract.clear(retryIntent) }
            )
        }
    }

    private fun saturatingTotalSize(files: List<SelectedFile>): Long? {
        var total = 0L
        files.forEach { file ->
            if (file.size < 0) return null
            total = try {
                Math.addExact(total, file.size)
            } catch (_: ArithmeticException) {
                Long.MAX_VALUE
            }
        }
        return total
    }

    private fun requestQrScan() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) ==
            PackageManager.PERMISSION_GRANTED
        ) launchQrScanner() else requestCameraPermission.launch(Manifest.permission.CAMERA)
    }

    private fun launchQrScanner() {
        scanQr.launch(
            QrScanConfiguration.applyTo(
                ScanOptions().setPrompt(getString(R.string.qr_scan_instruction))
            )
        )
    }

    private fun confirmCancelTransfer() {
        MaterialAlertDialogBuilder(this)
            .setTitle(R.string.cancel_transfer_title)
            .setMessage(R.string.cancel_transfer_message)
            .setNegativeButton(R.string.cancel, null)
            .setPositiveButton(R.string.cancel_transfer) { _, _ -> viewModel.cancelOutgoing() }
            .show()
    }

    private fun renderPairingOffer(offer: PairingOfferUi?) {
        if (offer == null) {
            pairingCountdown?.cancel()
            pairingCountdown = null
            pairingDialogPayload = null
            pairingDialog?.setOnDismissListener(null)
            pairingDialog?.dismiss()
            pairingDialog = null
            return
        }
        val dialog = pairingDialog?.takeIf { it.isShowing } ?: run {
            val content = layoutInflater.inflate(R.layout.dialog_pairing_qr, null)
            MaterialAlertDialogBuilder(this)
                .setTitle(R.string.connection_qr_title)
                .setView(content)
                .setPositiveButton(R.string.refresh_connection_qr, null)
                .setNegativeButton(android.R.string.cancel, null)
                .create()
                .also { created ->
                    created.setOnShowListener {
                        created.getButton(DialogInterface.BUTTON_POSITIVE).setOnClickListener {
                            viewModel.createPairingOffer()
                        }
                    }
                    created.setOnDismissListener {
                        val raw = pairingDialogPayload
                        pairingCountdown?.cancel()
                        pairingCountdown = null
                        pairingDialogPayload = null
                        pairingDialog = null
                        if (raw != null) viewModel.dismissPairingOffer(raw)
                    }
                    created.show()
                    pairingDialog = created
                }
        }
        if (pairingDialogPayload == offer.rawPayload) return
        pairingDialogPayload = offer.rawPayload
        dialog.findViewById<TextView>(R.id.pairingAddressText)?.text =
            getString(R.string.connection_qr_address, offer.address)
        val imageView = dialog.findViewById<ImageView>(R.id.pairingQrImage)
        dialog.findViewById<View>(R.id.pairingDialogContent)?.doOnLayout { content ->
            val density = resources.displayMetrics.density
            val size = QrDisplaySizer.side(
                availableWidthPx = content.width - content.paddingStart - content.paddingEnd,
                maximumPx = (280 * density).toInt(),
                fallbackPx = (240 * density).toInt()
            )
            imageView?.layoutParams = imageView?.layoutParams?.apply {
                width = size
                height = size
            }
            lifecycleScope.launch {
                val bitmap = withContext(Dispatchers.Default) {
                    QrBitmapEncoder.encode(offer.rawPayload, size)
                }
                if (pairingDialogPayload == offer.rawPayload) imageView?.setImageBitmap(bitmap)
            }
        }
        pairingCountdown?.cancel()
        val remaining = (offer.expiresAt - System.currentTimeMillis()).coerceAtLeast(0L)
        pairingCountdown = object : CountDownTimer(remaining, 1_000L) {
            override fun onTick(millisUntilFinished: Long) {
                val seconds = ((millisUntilFinished + 999L) / 1_000L).toInt()
                dialog.findViewById<TextView>(R.id.pairingCountdownText)?.text =
                    resources.getQuantityString(
                        R.plurals.connection_qr_expires,
                        seconds,
                        seconds
                    )
            }

            override fun onFinish() {
                viewModel.dismissPairingOffer(offer.rawPayload)
            }
        }.start()
    }

    private fun requestLegacyStoragePermissionIfNeeded() {
        if (Build.VERSION.SDK_INT <= Build.VERSION_CODES.P &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.WRITE_EXTERNAL_STORAGE) != PackageManager.PERMISSION_GRANTED
        ) requestStoragePermission.launch(Manifest.permission.WRITE_EXTERNAL_STORAGE)
    }

    private fun requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) requestNotificationPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
    }

}
