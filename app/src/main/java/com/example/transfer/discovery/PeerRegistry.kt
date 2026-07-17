package com.example.transfer.discovery

import java.net.InetAddress

class PeerRegistry(private val selfId: String) {
    private val peers = linkedMapOf<String, DiscoveredDevice>()

    @Synchronized
    fun update(packet: DiscoveryPacket, address: InetAddress, nowMillis: Long): Boolean {
        if (packet.id == selfId) return false
        val origin = peers[packet.id]?.origin ?: PeerOrigin.UDP
        peers[packet.id] = DiscoveredDevice(
            id = packet.id,
            name = packet.name,
            address = address,
            port = packet.port,
            lastSeenMillis = nowMillis,
            origin = origin
        )
        return true
    }

    @Synchronized
    fun addSessionPeer(device: DiscoveredDevice): Boolean {
        if (device.id == selfId) return false
        peers[device.id] = device.copy(origin = PeerOrigin.QR)
        return true
    }

    @Synchronized
    fun removeExpired(nowMillis: Long): Boolean {
        val sizeBefore = peers.size
        peers.entries.removeAll {
            it.value.origin == PeerOrigin.UDP &&
                nowMillis - it.value.lastSeenMillis > EXPIRY_MILLIS
        }
        return peers.size != sizeBefore
    }

    @Synchronized
    fun snapshot(): List<DiscoveredDevice> =
        peers.values.sortedWith(compareBy(String.CASE_INSENSITIVE_ORDER) { it.name })

    companion object {
        const val EXPIRY_MILLIS = 6_000L
    }
}
