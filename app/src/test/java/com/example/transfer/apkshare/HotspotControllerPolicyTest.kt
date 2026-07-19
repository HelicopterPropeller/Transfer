package com.example.transfer.apkshare

import android.net.wifi.WifiManager
import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
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

    @Test
    fun `result gate never publishes after close returns`() {
        val gate = HotspotResultGate()
        val callbackStarted = CountDownLatch(1)
        val releaseCallback = CountDownLatch(1)
        val closeReturned = CountDownLatch(1)
        val firstPublished = AtomicInteger()

        val publisher = Thread {
            gate.publish {
                callbackStarted.countDown()
                releaseCallback.await(2, TimeUnit.SECONDS)
                firstPublished.incrementAndGet()
            }
        }
        publisher.start()
        assertTrue(callbackStarted.await(2, TimeUnit.SECONDS))

        val closer = Thread {
            gate.close()
            closeReturned.countDown()
        }
        closer.start()
        assertFalse("close returned while a result callback was still publishing", closeReturned.await(100, TimeUnit.MILLISECONDS))

        releaseCallback.countDown()
        assertTrue(closeReturned.await(2, TimeUnit.SECONDS))
        publisher.join(2_000)
        closer.join(2_000)

        assertEquals(1, firstPublished.get())
        assertFalse(gate.publish { firstPublished.incrementAndGet() })
        assertEquals(1, firstPublished.get())
    }

    @Test
    fun `unexpected hotspot stop is delivered once only after a successful start`() {
        val gate = HotspotLifecycleGate()
        val losses = AtomicInteger()

        assertFalse(gate.publishUnexpectedStop { losses.incrementAndGet() })
        assertTrue(gate.markStarted())
        assertTrue(gate.publishUnexpectedStop { losses.incrementAndGet() })
        assertFalse(gate.publishUnexpectedStop { losses.incrementAndGet() })

        assertEquals(1, losses.get())
    }

    @Test
    fun `closing hotspot suppresses the platform stop callback`() {
        val gate = HotspotLifecycleGate()
        val losses = AtomicInteger()
        assertTrue(gate.markStarted())

        gate.close()

        assertFalse(gate.publishUnexpectedStop { losses.incrementAndGet() })
        assertEquals(0, losses.get())
    }
}

private class TestReservation : Closeable {
    val closeCount = AtomicInteger()

    override fun close() {
        closeCount.incrementAndGet()
    }
}
