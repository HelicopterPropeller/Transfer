package com.example.transfer.apkshare

import android.net.wifi.WifiManager
import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class HotspotControllerPolicyTest {
    @Test
    fun `api levels map to exact permission and fallback`() {
        assertEquals(HotspotRequirement.MANUAL, HotspotPermissionPolicy.requirementFor(24))
        assertEquals(HotspotRequirement.MANUAL, HotspotPermissionPolicy.requirementFor(25))
        assertEquals(HotspotRequirement.FINE_LOCATION, HotspotPermissionPolicy.requirementFor(26))
        assertEquals(HotspotRequirement.FINE_LOCATION, HotspotPermissionPolicy.requirementFor(32))
        assertEquals(HotspotRequirement.NEARBY_WIFI, HotspotPermissionPolicy.requirementFor(33))
        assertEquals(HotspotRequirement.NEARBY_WIFI, HotspotPermissionPolicy.requirementFor(36))
    }

    @Test
    fun `platform failures map to stable manual fallback reasons`() {
        assertEquals(
            HotspotFailure.TETHERING_DISALLOWED,
            HotspotFailure.fromPlatform(
                WifiManager.LocalOnlyHotspotCallback.ERROR_TETHERING_DISALLOWED,
            ),
        )
        assertEquals(
            HotspotFailure.INCOMPATIBLE_MODE,
            HotspotFailure.fromPlatform(
                WifiManager.LocalOnlyHotspotCallback.ERROR_INCOMPATIBLE_MODE,
            ),
        )
        assertEquals(
            HotspotFailure.NO_CHANNEL,
            HotspotFailure.fromPlatform(WifiManager.LocalOnlyHotspotCallback.ERROR_NO_CHANNEL),
        )
        assertEquals(
            HotspotFailure.GENERIC,
            HotspotFailure.fromPlatform(WifiManager.LocalOnlyHotspotCallback.ERROR_GENERIC),
        )
        assertEquals(HotspotFailure.GENERIC, HotspotFailure.fromPlatform(Int.MAX_VALUE))
    }

    @Test
    fun `credentials encode escaped wifi qr payload`() {
        val credentials = HotspotCredentials.create("Lab;\"Net", "p:a\\ss,word")

        assertEquals("Lab;\"Net", credentials.ssid)
        assertEquals("p:a\\ss,word", credentials.password)
        assertEquals(
            "WIFI:T:WPA;S:Lab\\;\\\"Net;P:p\\:a\\\\ss\\,word;H:false;;",
            credentials.wifiQrPayload,
        )
    }

    @Test
    fun `reservation owner keeps only first reservation and closes every rejected one`() {
        val owner = HotspotReservationOwner<TestReservation>()
        val first = TestReservation()
        val duplicate = TestReservation()

        assertTrue(owner.offer(first))
        assertFalse(owner.offer(duplicate))
        assertEquals(0, first.closeCount.get())
        assertEquals(1, duplicate.closeCount.get())

        owner.close()
        owner.close()
        assertEquals(1, first.closeCount.get())
    }

    @Test
    fun `reservation delivered after close is rejected and closed`() {
        val owner = HotspotReservationOwner<TestReservation>()
        val late = TestReservation()

        owner.close()

        assertFalse(owner.offer(late))
        assertEquals(1, late.closeCount.get())
    }
}

private class TestReservation : Closeable {
    val closeCount = AtomicInteger()

    override fun close() {
        closeCount.incrementAndGet()
    }
}
