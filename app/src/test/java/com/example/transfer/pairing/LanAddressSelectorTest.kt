package com.example.transfer.pairing

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class LanAddressSelectorTest {
    @Test
    fun `selects deterministic private ipv4 on wifi`() {
        val result = LanAddressSelector.select(
            LanNetworkSnapshot(
                transports = setOf(LanTransport.WIFI),
                addresses = listOf("192.168.4.20", "10.0.0.8", "fe80::1")
            )
        )

        assertEquals("10.0.0.8", result?.hostAddress)
    }

    @Test
    fun `rejects vpn cellular missing wifi and non-private addresses`() {
        assertNull(LanAddressSelector.select(LanNetworkSnapshot(setOf(LanTransport.VPN, LanTransport.WIFI), listOf("192.168.1.2"))))
        assertNull(LanAddressSelector.select(LanNetworkSnapshot(setOf(LanTransport.CELLULAR), listOf("10.0.0.2"))))
        assertNull(LanAddressSelector.select(LanNetworkSnapshot(setOf(LanTransport.WIFI), listOf("8.8.8.8", "127.0.0.1"))))
        assertNull(LanAddressSelector.select(null))
    }
}
