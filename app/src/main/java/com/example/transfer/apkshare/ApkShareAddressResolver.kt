package com.example.transfer.apkshare

import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.util.Collections

data class InterfaceAddressSnapshot(
    val interfaceName: String,
    val hostAddress: String,
    val prefixLength: Short,
)

data class SelectedLanAddress(
    val address: Inet4Address,
    val prefixLength: Short,
)

object ApkShareAddressResolver {
    private val excludedPrefixes = listOf("rmnet", "ccmni", "pdp", "tun", "ppp", "lo")

    fun existingWifi(addresses: List<InterfaceAddressSnapshot>): SelectedLanAddress? = addresses
        .asSequence()
        .filter { it.interfaceName.startsWith("wlan") || it.interfaceName.startsWith("wifi") }
        .mapNotNull { item ->
            privateIpv4(item)?.let { address -> SelectedLanAddress(address, item.prefixLength) }
        }
        .sortedBy { it.address.hostAddress }
        .firstOrNull()

    fun newHotspotAddress(
        before: List<InterfaceAddressSnapshot>,
        after: List<InterfaceAddressSnapshot>,
    ): SelectedLanAddress? {
        val oldAddresses = before
            .map { it.interfaceName to it.hostAddress }
            .toSet()

        return after
            .asSequence()
            .filter { (it.interfaceName to it.hostAddress) !in oldAddresses }
            .filterNot { item -> excludedPrefixes.any(item.interfaceName::startsWith) }
            .mapNotNull { item ->
                privateIpv4(item)?.let { address -> SelectedLanAddress(address, item.prefixLength) }
            }
            .sortedBy { it.address.hostAddress }
            .firstOrNull()
    }

    private fun privateIpv4(item: InterfaceAddressSnapshot): Inet4Address? =
        (InetAddress.getByName(item.hostAddress) as? Inet4Address)?.takeIf { address ->
            val bytes = address.address.map { it.toInt() and 0xff }
            bytes[0] == 10 ||
                bytes[0] == 172 && bytes[1] in 16..31 ||
                bytes[0] == 192 && bytes[1] == 168
        }
}

object InterfaceAddressProvider {
    fun snapshot(): List<InterfaceAddressSnapshot> =
        NetworkInterface.getNetworkInterfaces()
            ?.let(Collections::list)
            .orEmpty()
            .asSequence()
            .filter { it.isUp }
            .flatMap { networkInterface ->
                networkInterface.interfaceAddresses
                    .asSequence()
                    .mapNotNull { interfaceAddress ->
                        val address = interfaceAddress.address as? Inet4Address
                            ?: return@mapNotNull null
                        InterfaceAddressSnapshot(
                            interfaceName = networkInterface.name,
                            hostAddress = address.hostAddress ?: return@mapNotNull null,
                            prefixLength = interfaceAddress.networkPrefixLength,
                        )
                    }
            }
            .toList()
}
