package com.example.transfer.pairing

import com.journeyapps.barcodescanner.ScanOptions

internal data class QrScanSettings(
    val formats: List<String>,
    val orientationLocked: Boolean,
    val beepEnabled: Boolean,
)

internal object QrScanConfiguration {
    val settings = QrScanSettings(
        formats = listOf(ScanOptions.QR_CODE),
        orientationLocked = true,
        beepEnabled = false,
    )

    fun applyTo(options: ScanOptions): ScanOptions = options
        .setDesiredBarcodeFormats(settings.formats)
        .setOrientationLocked(settings.orientationLocked)
        .setBeepEnabled(settings.beepEnabled)
        .setCaptureActivity(QrScanActivity::class.java)
}
