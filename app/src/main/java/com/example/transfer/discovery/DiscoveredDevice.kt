package com.example.transfer.discovery

import java.net.InetAddress

data class DiscoveryPacket(
    val id: String,
    val name: String,
    val port: Int
)

enum class PeerOrigin { UDP, QR }

data class DiscoveredDevice(
    val id: String,
    val name: String,
    val address: InetAddress,
    val port: Int,
    val lastSeenMillis: Long,
    val origin: PeerOrigin = PeerOrigin.UDP
)
