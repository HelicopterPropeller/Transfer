package com.example.transfer.apkshare

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class ApkShareAddressResolverTest {
    @Test
    fun `existing wifi excludes cellular vpn loopback and public addresses`() {
        val addresses = listOf(
            InterfaceAddressSnapshot("rmnet_data0", "10.20.0.2", 24),
            InterfaceAddressSnapshot("tun0", "10.8.0.2", 24),
            InterfaceAddressSnapshot("lo", "127.0.0.1", 8),
            InterfaceAddressSnapshot("wlan0", "8.8.8.8", 24),
            InterfaceAddressSnapshot("wlan1", "192.168.1.8", 24),
        )

        assertEquals(
            "192.168.1.8",
            ApkShareAddressResolver.existingWifi(addresses)?.address?.hostAddress,
        )
    }

    @Test
    fun `existing wifi selection is deterministic`() {
        val addresses = listOf(
            InterfaceAddressSnapshot("wlan1", "192.168.1.8", 24),
            InterfaceAddressSnapshot("wifi0", "192.168.1.7", 24),
        )

        assertEquals(
            "192.168.1.7",
            ApkShareAddressResolver.existingWifi(addresses)?.address?.hostAddress,
        )
    }

    @Test
    fun `new hotspot address is selected deterministically from post-start delta`() {
        val before = listOf(
            InterfaceAddressSnapshot("wlan0", "192.168.1.8", 24),
        )
        val after = before + listOf(
            InterfaceAddressSnapshot("ap1", "192.168.50.1", 24),
            InterfaceAddressSnapshot("ap0", "192.168.43.1", 24),
        )

        assertEquals(
            "192.168.43.1",
            ApkShareAddressResolver.newHotspotAddress(before, after)?.address?.hostAddress,
        )
    }

    @Test
    fun `selected address preserves network prefix length`() {
        val addresses = listOf(
            InterfaceAddressSnapshot("wlan0", "10.42.0.1", 28),
        )

        assertEquals(
            28.toShort(),
            ApkShareAddressResolver.existingWifi(addresses)?.prefixLength,
        )
    }

    @Test
    fun `existing wifi returns null when there is no candidate`() {
        val addresses = listOf(
            InterfaceAddressSnapshot("rmnet_data0", "10.20.0.2", 24),
            InterfaceAddressSnapshot("wlan0", "203.0.113.8", 24),
        )

        assertNull(ApkShareAddressResolver.existingWifi(addresses))
    }

    @Test
    fun `new hotspot address returns null when post-start has no new candidate`() {
        val before = listOf(
            InterfaceAddressSnapshot("wlan0", "192.168.1.8", 24),
        )
        val after = before + listOf(
            InterfaceAddressSnapshot("tun0", "10.8.0.2", 24),
            InterfaceAddressSnapshot("ap0", "198.51.100.1", 24),
        )

        assertNull(ApkShareAddressResolver.newHotspotAddress(before, after))
    }
}
