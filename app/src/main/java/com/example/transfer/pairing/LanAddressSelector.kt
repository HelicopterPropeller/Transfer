package com.example.transfer.pairing

import android.content.Context
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import java.net.Inet4Address

enum class LanTransport { WIFI, CELLULAR, VPN }

data class LanNetworkSnapshot(
    val transports: Set<LanTransport>,
    val addresses: List<String>
)

object LanAddressSelector {
    fun select(snapshot: LanNetworkSnapshot?): Inet4Address? {
        snapshot ?: return null
        if (LanTransport.WIFI !in snapshot.transports ||
            LanTransport.VPN in snapshot.transports ||
            LanTransport.CELLULAR in snapshot.transports
        ) return null
        return snapshot.addresses
            .mapNotNull(::canonicalPrivateIpv4)
            .sortedBy { address -> address.address.joinToString("") { "%03d".format(it.toInt() and 0xff) } }
            .firstOrNull()
    }
}

class WifiLanAddressProvider(context: Context) {
    private val connectivityManager = context.applicationContext
        .getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager

    fun currentPrivateIpv4(): Inet4Address? {
        val network = connectivityManager.activeNetwork ?: return null
        val capabilities = connectivityManager.getNetworkCapabilities(network) ?: return null
        val transports = buildSet {
            if (capabilities.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)) add(LanTransport.WIFI)
            if (capabilities.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)) add(LanTransport.CELLULAR)
            if (capabilities.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) add(LanTransport.VPN)
        }
        val addresses = connectivityManager.getLinkProperties(network)
            ?.linkAddresses
            .orEmpty()
            .mapNotNull { it.address.hostAddress }
        return LanAddressSelector.select(LanNetworkSnapshot(transports, addresses))
    }
}
