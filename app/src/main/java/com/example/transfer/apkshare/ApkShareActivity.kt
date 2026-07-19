package com.example.transfer.apkshare

import android.Manifest
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.Bundle
import android.os.IBinder
import android.provider.Settings
import android.text.format.Formatter
import android.view.View
import android.widget.ImageView
import android.widget.ProgressBar
import android.widget.TextView
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.doOnLayout
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.example.transfer.R
import com.example.transfer.pairing.QrBitmapEncoder
import com.example.transfer.pairing.QrDisplaySizer
import com.google.android.material.appbar.MaterialToolbar
import com.google.android.material.button.MaterialButton
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class ApkShareActivity : AppCompatActivity() {
    private var binder: ApkShareForegroundService.LocalBinder? = null
    private var bound = false
    private var collectorJob: Job? = null
    private var qrJob: Job? = null
    private var currentState: ApkShareState = ApkShareState.Idle
    private var currentUi = ApkShareUiModel.from(currentState, System.currentTimeMillis())
    private var lastQrPayload: String? = null
    private var lastQrSide = 0
    private var pendingPermissionRequirement: HotspotRequirement? = null

    private lateinit var content: View
    private lateinit var titleText: TextView
    private lateinit var detailText: TextView
    private lateinit var qrImage: ImageView
    private lateinit var credentialsText: TextView
    private lateinit var metadataText: TextView
    private lateinit var progress: ProgressBar
    private lateinit var primaryButton: MaterialButton
    private lateinit var secondaryButton: MaterialButton

    private val connection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
            binder = service as? ApkShareForegroundService.LocalBinder
            collectorJob?.cancel()
            val connected = binder ?: return
            collectorJob = lifecycleScope.launch {
                repeatOnLifecycle(Lifecycle.State.STARTED) {
                    connected.state.collect { state ->
                        currentState = state
                        render(state)
                    }
                }
            }
        }

        override fun onServiceDisconnected(name: ComponentName?) {
            collectorJob?.cancel()
            collectorJob = null
            binder = null
        }
    }

    private val requestHotspotPermissions = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions(),
    ) { grants ->
        val requirement = pendingPermissionRequirement.also {
            pendingPermissionRequirement = null
        }
        val granted = when (requirement) {
            HotspotRequirement.FINE_LOCATION ->
                grants[Manifest.permission.ACCESS_FINE_LOCATION] == true

            HotspotRequirement.NEARBY_WIFI -> grants[NEARBY_WIFI_PERMISSION] == true
            HotspotRequirement.MANUAL,
            null,
            -> false
        }
        if (granted) binder?.startAutomaticHotspot()
        else binder?.requestManualHotspotFallback()
    }

    private val hotspotSettings = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult(),
    ) {
        binder?.continueAfterManualHotspot()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContentView(R.layout.activity_apk_share)
        bindViews()
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.apkShareRoot)) { view, insets ->
            val bars = insets.getInsets(WindowInsetsCompat.Type.systemBars())
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            insets
        }
        findViewById<MaterialToolbar>(R.id.apkShareToolbar)
            .setNavigationOnClickListener { finish() }
        primaryButton.setOnClickListener { currentUi.primaryAction?.let(::performAction) }
        secondaryButton.setOnClickListener { currentUi.secondaryAction?.let(::performAction) }
        findViewById<MaterialButton>(R.id.cancelApkShareButton).setOnClickListener {
            binder?.cancel()
            finish()
        }
        content.doOnLayout { renderQr(currentUi.qrPayload) }
        render(currentState)

        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                while (isActive) {
                    delay(1_000L)
                    if (currentState is ApkShareState.ReadyToDownload) render(currentState)
                }
            }
        }
    }

    override fun onStart() {
        super.onStart()
        val intent = Intent(this, ApkShareForegroundService::class.java)
        ContextCompat.startForegroundService(this, intent)
        bound = bindService(intent, connection, Context.BIND_AUTO_CREATE)
    }

    override fun onStop() {
        collectorJob?.cancel()
        collectorJob = null
        if (qrJob?.isActive == true) {
            qrJob?.cancel()
            lastQrPayload = null
            lastQrSide = 0
        }
        if (bound) {
            unbindService(connection)
            bound = false
        }
        binder = null
        super.onStop()
    }

    private fun bindViews() {
        content = findViewById(R.id.apkShareContent)
        titleText = findViewById(R.id.stateTitleText)
        detailText = findViewById(R.id.stateDetailText)
        qrImage = findViewById(R.id.apkShareQrImage)
        credentialsText = findViewById(R.id.hotspotCredentialsText)
        metadataText = findViewById(R.id.artifactMetadataText)
        progress = findViewById(R.id.apkShareProgress)
        primaryButton = findViewById(R.id.primaryActionButton)
        secondaryButton = findViewById(R.id.secondaryActionButton)
    }

    private fun render(state: ApkShareState) {
        val ui = ApkShareUiModel.from(state, System.currentTimeMillis())
        currentUi = ui
        titleText.setText(ui.titleRes)
        detailText.text = if (state is ApkShareState.ManualHotspotRequired) {
            getString(hotspotFailureMessage(state.reason))
        } else {
            getString(ui.detailRes, *ui.detailArgs.toTypedArray())
        }
        renderQr(ui.qrPayload)

        credentialsText.visibility = visibleIf(ui.showCredentials)
        ui.credentials?.let { credentials ->
            credentialsText.text = getString(
                R.string.apk_share_credentials,
                credentials.ssid,
                credentials.password,
            )
        }
        metadataText.visibility = visibleIf(ui.showArtifactMetadata)
        ui.artifact?.let { artifact ->
            metadataText.text = getString(
                R.string.apk_share_artifact_metadata,
                artifact.fileName,
                artifact.versionName,
                artifact.versionCode,
                Formatter.formatFileSize(this, artifact.size),
                artifact.sha256,
            )
        }

        progress.visibility = visibleIf(ui.progress != null)
        ui.progress?.let(progress::setProgress)
        renderAction(primaryButton, ui.primaryAction)
        renderAction(secondaryButton, ui.secondaryAction)
        if (ui.permissionRequirement != null) {
            primaryButton.setText(R.string.apk_share_grant_wifi_permission)
        }
    }

    private fun renderQr(payload: String?) {
        qrImage.visibility = visibleIf(payload != null)
        if (payload == null) {
            qrJob?.cancel()
            qrImage.setImageDrawable(null)
            lastQrPayload = null
            lastQrSide = 0
            return
        }
        val maximum = (360 * resources.displayMetrics.density).toInt().coerceAtLeast(1)
        val fallback = (resources.displayMetrics.widthPixels -
            (40 * resources.displayMetrics.density).toInt()).coerceAtLeast(1)
        val available = (content.width - content.paddingStart - content.paddingEnd).coerceAtLeast(0)
        val side = QrDisplaySizer.side(available, maximum, fallback)
        if (payload == lastQrPayload && side == lastQrSide) return
        qrImage.layoutParams = qrImage.layoutParams.apply { height = side }
        lastQrPayload = payload
        lastQrSide = side
        qrJob?.cancel()
        qrJob = lifecycleScope.launch {
            val bitmap = withContext(Dispatchers.Default) {
                QrBitmapEncoder.encode(payload, side)
            }
            if (lastQrPayload == payload && lastQrSide == side) {
                qrImage.setImageBitmap(bitmap)
            }
        }
    }

    private fun renderAction(button: MaterialButton, action: ApkSharePrimaryAction?) {
        button.visibility = visibleIf(action != null)
        button.text = action?.let { getString(actionLabel(it)) }.orEmpty()
    }

    private fun performAction(action: ApkSharePrimaryAction) {
        when (action) {
            ApkSharePrimaryAction.START_EXISTING_LAN -> binder?.startExistingLan()
            ApkSharePrimaryAction.START_HOTSPOT -> requestOrStartHotspot()
            ApkSharePrimaryAction.SHOW_DOWNLOAD_QR -> binder?.showDownloadQr()
            ApkSharePrimaryAction.OPEN_HOTSPOT_SETTINGS -> openHotspotSettings()
            ApkSharePrimaryAction.RETRY -> retryCurrentOperation()
        }
    }

    private fun requestOrStartHotspot() {
        val requirement = currentUi.permissionRequirement
        val permissions = when (requirement) {
            HotspotRequirement.FINE_LOCATION -> if (
                android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.S
            ) {
                arrayOf(
                    Manifest.permission.ACCESS_COARSE_LOCATION,
                    Manifest.permission.ACCESS_FINE_LOCATION,
                )
            } else {
                arrayOf(Manifest.permission.ACCESS_FINE_LOCATION)
            }

            HotspotRequirement.NEARBY_WIFI -> arrayOf(NEARBY_WIFI_PERMISSION)
            HotspotRequirement.MANUAL -> null
            null -> {
                binder?.startAutomaticHotspot()
                return
            }
        }
        if (permissions == null) {
            binder?.requestManualHotspotFallback()
        } else {
            pendingPermissionRequirement = requirement
            requestHotspotPermissions.launch(permissions)
        }
    }

    private fun retryCurrentOperation() {
        when (currentUi.retryAction) {
            RetryAction.PREPARE_EXISTING_LAN -> binder?.startExistingLan()
            RetryAction.PREPARE_HOTSPOT -> binder?.startAutomaticHotspot()
            RetryAction.RESOLVE_MANUAL_HOTSPOT -> binder?.continueAfterManualHotspot()
            null -> Unit
        }
    }

    private fun openHotspotSettings() {
        val tether = Intent("android.settings.TETHER_SETTINGS")
        val intent = if (tether.resolveActivity(packageManager) != null) {
            tether
        } else {
            Intent(Settings.ACTION_WIRELESS_SETTINGS)
        }
        hotspotSettings.launch(intent)
    }

    private fun actionLabel(action: ApkSharePrimaryAction): Int = when (action) {
        ApkSharePrimaryAction.START_EXISTING_LAN -> R.string.apk_share_use_current_wifi
        ApkSharePrimaryAction.START_HOTSPOT -> R.string.apk_share_create_hotspot
        ApkSharePrimaryAction.SHOW_DOWNLOAD_QR -> R.string.apk_share_continue_download_qr
        ApkSharePrimaryAction.OPEN_HOTSPOT_SETTINGS -> R.string.apk_share_open_hotspot_settings
        ApkSharePrimaryAction.RETRY -> R.string.apk_share_retry
    }

    private fun hotspotFailureMessage(failure: HotspotFailure): Int = when (failure) {
        HotspotFailure.USER_REJECTED -> R.string.apk_share_failure_user_rejected
        HotspotFailure.TETHERING_DISALLOWED -> R.string.apk_share_failure_tethering_disallowed
        HotspotFailure.INCOMPATIBLE_MODE -> R.string.apk_share_failure_incompatible_mode
        HotspotFailure.NO_CHANNEL -> R.string.apk_share_failure_no_channel
        HotspotFailure.GENERIC -> R.string.apk_share_failure_generic
    }

    private fun visibleIf(visible: Boolean): Int = if (visible) View.VISIBLE else View.GONE

    private companion object {
        const val NEARBY_WIFI_PERMISSION = "android.permission.NEARBY_WIFI_DEVICES"
    }
}
