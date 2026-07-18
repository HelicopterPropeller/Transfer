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
            InterfaceAddressSnapshot("WlAn1", "192.168.1.8", 24),
            InterfaceAddressSnapshot("WiFi0", "192.168.1.7", 24),
        )

        assertEquals(
            "192.168.1.7",
            ApkShareAddressResolver.existingWifi(addresses)?.address?.hostAddress,
        )
    }

    @Test
    fun `trusted mixed-case hotspot wins over new vpn and unknown interfaces`() {
        val before = listOf(
            InterfaceAddressSnapshot("wlan0", "192.168.1.8", 24),
        )
        val after = before + listOf(
            InterfaceAddressSnapshot(
                "corporate0",
                "10.8.0.2",
                24,
                transport = InterfaceTransport.VPN,
            ),
            InterfaceAddressSnapshot("wg0", "10.9.0.1", 24),
            InterfaceAddressSnapshot("SoFtAp0", "192.168.43.1", 24),
        )

        assertEquals(
            "192.168.43.1",
            ApkShareAddressResolver.newHotspotAddress(before, after)?.address?.hostAddress,
        )
    }

    @Test
    fun `two trusted hotspot candidates are ambiguous`() {
        val after = listOf(
            InterfaceAddressSnapshot("ap0", "192.168.43.1", 24),
            InterfaceAddressSnapshot("swlan0", "192.168.50.1", 24),
        )

        assertNull(ApkShareAddressResolver.newHotspotAddress(emptyList(), after))
    }

    @Test
    fun `existing wifi preserves network prefix length`() {
        val addresses = listOf(
            InterfaceAddressSnapshot("wlan0", "10.42.0.1", 28),
        )

        assertEquals(
            28.toShort(),
            ApkShareAddressResolver.existingWifi(addresses)?.prefixLength,
        )
    }

    @Test
    fun `hotspot preserves network prefix length`() {
        val after = listOf(
            InterfaceAddressSnapshot("ap0", "192.168.43.1", 27),
        )

        assertEquals(
            27.toShort(),
            ApkShareAddressResolver.newHotspotAddress(emptyList(), after)?.prefixLength,
        )
    }

    @Test
    fun `explicit wifi transport allows a nonstandard interface name`() {
        val addresses = listOf(
            InterfaceAddressSnapshot(
                "mlan0",
                "192.168.4.2",
                24,
                transport = InterfaceTransport.WIFI,
            ),
        )

        assertEquals(
            "192.168.4.2",
            ApkShareAddressResolver.existingWifi(addresses)?.address?.hostAddress,
        )
    }

    @Test
    fun `existing wifi rejects explicit cellular and vpn transports despite wifi-like names`() {
        val addresses = listOf(
            InterfaceAddressSnapshot(
                "wlanCellular",
                "192.168.4.2",
                24,
                transport = InterfaceTransport.CELLULAR,
            ),
            InterfaceAddressSnapshot(
                "wifiCorporate",
                "192.168.5.2",
                24,
                transport = InterfaceTransport.VPN,
            ),
        )

        assertNull(ApkShareAddressResolver.existingWifi(addresses))
    }

    @Test
    fun `hotspot rejects explicit cellular and vpn transports despite hotspot-like names`() {
        val after = listOf(
            InterfaceAddressSnapshot(
                "apCellular",
                "192.168.43.1",
                24,
                transport = InterfaceTransport.CELLULAR,
            ),
            InterfaceAddressSnapshot(
                "softapCorporate",
                "192.168.50.1",
                24,
                transport = InterfaceTransport.VPN,
            ),
        )

        assertNull(ApkShareAddressResolver.newHotspotAddress(emptyList(), after))
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
    fun `new hotspot address returns null when delta has only untrusted unknown interfaces`() {
        val before = listOf(
            InterfaceAddressSnapshot("wlan0", "192.168.1.8", 24),
        )
        val after = before + listOf(
            InterfaceAddressSnapshot("wg0", "10.8.0.2", 24),
            InterfaceAddressSnapshot("rndis0", "192.168.42.1", 24),
        )

        assertNull(ApkShareAddressResolver.newHotspotAddress(before, after))
    }

    @Test
    fun `existing wifi rejects hostnames and malformed numeric addresses`() {
        val addresses = listOf(
            InterfaceAddressSnapshot("wlan0", "localhost", 24),
            InterfaceAddressSnapshot("wlan1", "192.168.1", 24),
            InterfaceAddressSnapshot("wlan2", "999.168.1.1", 24),
            InterfaceAddressSnapshot("wlan3", "192.168.1.1.5", 24),
        )

        assertNull(ApkShareAddressResolver.existingWifi(addresses))
    }

    @Test
    fun `hotspot rejects hostnames and malformed numeric addresses`() {
        val after = listOf(
            InterfaceAddressSnapshot("ap0", "localhost", 24),
            InterfaceAddressSnapshot("softap0", "192.168.1", 24),
            InterfaceAddressSnapshot("swlan0", "192.168.-1.1", 24),
        )

        assertNull(ApkShareAddressResolver.newHotspotAddress(emptyList(), after))
    }
}
