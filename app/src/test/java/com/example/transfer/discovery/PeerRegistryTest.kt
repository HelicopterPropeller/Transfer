package com.example.transfer.discovery

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.InetAddress

class PeerRegistryTest {
    @Test
    fun `registry ignores self refreshes peers sorts and expires`() {
        val registry = PeerRegistry("self")
        val firstAddress = InetAddress.getByName("192.168.1.8")
        val newAddress = InetAddress.getByName("192.168.1.9")

        assertFalse(registry.update(DiscoveryPacket("self", "Me", 42043), firstAddress, 0))
        assertTrue(registry.update(DiscoveryPacket("z", "Zulu", 42043), firstAddress, 100))
        assertTrue(registry.update(DiscoveryPacket("a", "Alpha", 42043), firstAddress, 200))
        assertTrue(registry.update(DiscoveryPacket("z", "Zulu 2", 42044), newAddress, 300))

        val devices = registry.snapshot()
        assertEquals(listOf("Alpha", "Zulu 2"), devices.map { it.name })
        assertEquals(newAddress, devices.last().address)
        assertEquals(300, devices.last().lastSeenMillis)

        assertFalse(registry.removeExpired(6_200))
        assertTrue(registry.removeExpired(6_301))
        assertEquals(emptyList<DiscoveredDevice>(), registry.snapshot())
    }

    @Test
    fun `qr peers survive udp expiry and udp refresh preserves qr origin`() {
        val registry = PeerRegistry("self")
        val qrAddress = InetAddress.getByName("192.168.1.8")
        val udpAddress = InetAddress.getByName("192.168.1.9")

        assertTrue(registry.addSessionPeer(
            DiscoveredDevice("peer", "QR Peer", qrAddress, 42043, 0, PeerOrigin.QR)
        ))
        assertFalse(registry.removeExpired(100_000))
        assertEquals(PeerOrigin.QR, registry.snapshot().single().origin)

        assertTrue(registry.update(DiscoveryPacket("peer", "UDP Peer", 42044), udpAddress, 100_001))
        val refreshed = registry.snapshot().single()
        assertEquals(PeerOrigin.QR, refreshed.origin)
        assertEquals(udpAddress, refreshed.address)
        assertFalse(registry.removeExpired(200_000))
    }

    @Test
    fun `registry rejects qr self peer`() {
        val registry = PeerRegistry("self")
        assertFalse(registry.addSessionPeer(
            DiscoveredDevice(
                "self", "Me", InetAddress.getByName("192.168.1.2"), 42043, 0, PeerOrigin.QR
            )
        ))
    }
}
