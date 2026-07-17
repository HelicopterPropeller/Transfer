package com.example.transfer.pairing

import android.view.View
import com.example.transfer.R
import com.journeyapps.barcodescanner.CaptureActivity
import com.journeyapps.barcodescanner.DecoratedBarcodeView

class QrScanActivity : CaptureActivity() {
    override fun initializeContent(): DecoratedBarcodeView {
        setContentView(R.layout.activity_qr_scan)
        findViewById<View>(R.id.qrScanCloseButton).setOnClickListener { finish() }
        return findViewById(R.id.zxing_barcode_scanner)
    }
}
