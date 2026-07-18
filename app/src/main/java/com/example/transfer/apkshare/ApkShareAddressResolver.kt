package com.example.transfer.apkshare

import android.content.Context
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.util.Collections
import java.util.Locale

enum class InterfaceTransport {
    WIFI,
    CELLULAR,
    VPN,
    UNKNOWN,
}

data class InterfaceAddressSnapshot(
    val interfaceName: String,
    val hostAddress: String,
    val prefixLength: Short,
    val transport: InterfaceTransport = InterfaceTransport.UNKNOWN,
)

data class SelectedLanAddress(
    val address: Inet4Address,
    val prefixLength: Short,
)

object ApkShareAddressResolver {
    private val hotspotInterfaceName =
        Regex("^(?:softap|swlan|wlan|wifi|ap)(?:\\d+|[_-].*)?$")

    fun existingWifi(addresses: List<InterfaceAddressSnapshot>): SelectedLanAddress? = addresses
        .asSequence()
        .filter { item ->
            when (item.transport) {
                InterfaceTransport.WIFI -> true
                InterfaceTransport.CELLULAR,
                InterfaceTransport.VPN,
                -> false
                InterfaceTransport.UNKNOWN -> item.interfaceName
                    .lowercase(Locale.ROOT)
                    .let { it.startsWith("wlan") || it.startsWith("wifi") }
            }
        }
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
            .filterNot { item ->
                item.transport == InterfaceTransport.CELLULAR ||
                    item.transport == InterfaceTransport.VPN
            }
            .filter { item ->
                item.transport == InterfaceTransport.WIFI ||
                    hotspotInterfaceName.matches(item.interfaceName.lowercase(Locale.ROOT))
            }
            .mapNotNull { item ->
                privateIpv4(item)?.let { address -> SelectedLanAddress(address, item.prefixLength) }
            }
            .toList()
            .singleOrNull()
    }

    private fun privateIpv4(item: InterfaceAddressSnapshot): Inet4Address? {
        val parts = item.hostAddress.split('.', limit = 5)
        if (parts.size != 4) return null

        val bytes = ByteArray(4)
        parts.forEachIndexed { index, part ->
            if (part.isEmpty() || part.any { it !in '0'..'9' }) return null
            val value = part.toIntOrNull()?.takeIf { it in 0..255 } ?: return null
            bytes[index] = value.toByte()
        }

        val address = InetAddress.getByAddress(bytes) as Inet4Address
        val octets = bytes.map { it.toInt() and 0xff }
        return address.takeIf {
            octets[0] == 10 ||
                octets[0] == 172 && octets[1] in 16..31 ||
                octets[0] == 192 && octets[1] == 168
        }
    }
}

object InterfaceAddressProvider {
    fun snapshot(context: Context): List<InterfaceAddressSnapshot> {
        val transports = interfaceTransports(context)
        val interfaces = try {
            val enumeration = NetworkInterface.getNetworkInterfaces() ?: return emptyList()
            Collections.list(enumeration)
        } catch (_: Exception) {
            return emptyList()
        }
        val snapshots = mutableListOf<InterfaceAddressSnapshot>()

        for (networkInterface in interfaces) {
            val interfaceData = try {
                if (!networkInterface.isUp) continue
                val interfaceName = networkInterface.name ?: continue
                interfaceName to networkInterface.interfaceAddresses.toList()
            } catch (_: Exception) {
                continue
            }
            val (interfaceName, addresses) = interfaceData
            val transport = transports[interfaceName.lowercase(Locale.ROOT)]
                ?: InterfaceTransport.UNKNOWN

            for (interfaceAddress in addresses) {
                try {
                    val address = interfaceAddress.address as? Inet4Address
                        ?: continue
                    val hostAddress = address.hostAddress ?: continue
                    snapshots += InterfaceAddressSnapshot(
                        interfaceName = interfaceName,
                        hostAddress = hostAddress,
                        prefixLength = interfaceAddress.networkPrefixLength,
                        transport = transport,
                    )
                } catch (_: Exception) {
                    // Skip only the unreadable address and continue snapshotting the interface.
                }
            }
        }

        return snapshots.toList()
    }

    @Suppress("DEPRECATION")
    private fun interfaceTransports(context: Context): Map<String, InterfaceTransport> {
        val manager = connectivityRead {
            context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
        } ?: return emptyMap()
        // This one-shot API needs a synchronous snapshot; callbacks would outlive the call.
        val networks = connectivityRead { manager.allNetworks } ?: return emptyMap()
        val result = mutableMapOf<String, InterfaceTransport>()

        networks.forEach { network ->
            val interfaceName = connectivityRead { manager.getLinkProperties(network) }
                ?.interfaceName
                ?: return@forEach
            val capabilities = connectivityRead { manager.getNetworkCapabilities(network) }
                ?: return@forEach
            val discovered = capabilities.transport()
            val key = interfaceName.lowercase(Locale.ROOT)
            val current = result[key] ?: InterfaceTransport.UNKNOWN
            if (discovered.priority > current.priority) {
                result[key] = discovered
            }
        }

        return result.toMap()
    }

    private fun NetworkCapabilities.transport(): InterfaceTransport = when {
        hasTransport(NetworkCapabilities.TRANSPORT_VPN) -> InterfaceTransport.VPN
        hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> InterfaceTransport.CELLULAR
        hasTransport(NetworkCapabilities.TRANSPORT_WIFI) -> InterfaceTransport.WIFI
        else -> InterfaceTransport.UNKNOWN
    }

    private val InterfaceTransport.priority: Int
        get() = when (this) {
            InterfaceTransport.VPN -> 3
            InterfaceTransport.CELLULAR -> 2
            InterfaceTransport.WIFI -> 1
            InterfaceTransport.UNKNOWN -> 0
        }

    private inline fun <T> connectivityRead(block: () -> T): T? = try {
        block()
    } catch (_: SecurityException) {
        null
    } catch (_: RuntimeException) {
        null
    }
}
