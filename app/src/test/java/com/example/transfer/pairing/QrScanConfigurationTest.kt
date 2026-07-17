package com.example.transfer.pairing

import com.journeyapps.barcodescanner.ScanOptions
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class QrScanConfigurationTest {
    @Test
    fun scannerUsesOnlyQrCodesInLockedPortraitWithoutBeep() {
        val settings = QrScanConfiguration.settings

        assertEquals(listOf(ScanOptions.QR_CODE), settings.formats)
        assertTrue(settings.orientationLocked)
        assertFalse(settings.beepEnabled)
    }
}
