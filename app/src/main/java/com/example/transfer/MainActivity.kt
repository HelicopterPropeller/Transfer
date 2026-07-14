package com.example.transfer

import android.Manifest
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import android.text.format.Formatter
import android.view.LayoutInflater
import android.view.View
import android.widget.LinearLayout
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
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.example.transfer.history.HistoryActivity
import com.example.transfer.history.HistoryRetryContract
import com.example.transfer.service.TransferForegroundService
import com.example.transfer.service.TransferServiceApi
import com.example.transfer.ui.LatestSelectionRequest
import com.example.transfer.ui.SelectedFile
import com.example.transfer.ui.SelectedFileResolver
import com.example.transfer.ui.TransferUiState
import com.google.android.material.button.MaterialButton
import com.google.android.material.card.MaterialCardView
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class MainActivity : AppCompatActivity() {
    private val viewModel: TransferViewModel by viewModels()
    private val latestSelectionRequest = LatestSelectionRequest()
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
    private lateinit var transferCard: MaterialCardView
    private lateinit var transferTitleText: TextView
    private lateinit var transferFileText: TextView
    private lateinit var transferBatchText: TextView
    private lateinit var transferProgress: ProgressBar
    private lateinit var transferMessageText: TextView
    private lateinit var pauseResumeButton: MaterialButton

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
        restoreHistoryRetry(intent)
        findViewById<MaterialButton>(R.id.selectFileButton).setOnClickListener { openDocuments.launch(arrayOf("*/*")) }
        sendButton.setOnClickListener { viewModel.sendSelected() }
        pauseResumeButton.setOnClickListener { viewModel.togglePause() }
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
        transferCard = findViewById(R.id.transferCard)
        transferTitleText = findViewById(R.id.transferTitleText)
        transferFileText = findViewById(R.id.transferFileText)
        transferBatchText = findViewById(R.id.transferBatchText)
        transferProgress = findViewById(R.id.transferProgress)
        transferMessageText = findViewById(R.id.transferMessageText)
        pauseResumeButton = findViewById(R.id.pauseResumeButton)
    }

    private fun render(state: TransferUiState) {
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
        state.transfer.let { transfer ->
            transferCard.visibility = if (transfer == null) View.GONE else View.VISIBLE
            if (transfer != null) {
                transferTitleText.text = if (transfer.direction.isBlank()) transfer.message else getString(R.string.transfer_title, transfer.direction)
                transferFileText.text = transfer.fileName
                transferFileText.visibility = if (transfer.fileName.isBlank()) View.GONE else View.VISIBLE
                transferBatchText.text = getString(
                    R.string.transfer_batch,
                    transfer.fileIndex,
                    transfer.fileCount,
                    transfer.batchProgress
                )
                transferBatchText.visibility = if (transfer.active) View.VISIBLE else View.GONE
                transferProgress.progress = transfer.progress
                transferProgress.visibility = if (transfer.active) View.VISIBLE else View.GONE
                transferMessageText.text = getString(R.string.transfer_message, transfer.progress, transfer.message)
                pauseResumeButton.visibility = if (state.canPause || state.canResume) View.VISIBLE else View.GONE
                pauseResumeButton.setText(if (state.canPause) R.string.pause else R.string.resume)
            }
        }
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
