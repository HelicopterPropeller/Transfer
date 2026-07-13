package com.example.transfer.discovery

import java.net.InetAddress

data class DiscoveryPacket(
    val id: String,
    val name: String,
    val port: Int
)

data class DiscoveredDevice(
    val id: String,
    val name: String,
    val address: InetAddress,
    val port: Int,
    val lastSeenMillis: Long
)
