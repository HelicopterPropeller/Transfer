package com.example.transfer.apkshare

import org.junit.Assert.assertEquals
import org.junit.Test

class WifiQrPayloadTest {
    @Test
    fun `wifi qr escapes reserved characters`() {
        assertEquals(
            "WIFI:T:WPA;S:Lab\\;Net;P:p\\:a\\\\ss;H:false;;",
            WifiQrPayload.encode("Lab;Net", "p:a\\ss", hidden = false),
        )
    }
}
