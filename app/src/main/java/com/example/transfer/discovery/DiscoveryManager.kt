package com.example.transfer.discovery

import android.content.Context
import android.net.wifi.WifiManager
import com.example.transfer.transfer.FileTransferServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.SocketTimeoutException
import java.util.Collections

class DiscoveryManager(
    context: Context,
    private val deviceId: String,
    private val deviceName: String,
    private val tcpPort: Int = FileTransferServer.DEFAULT_PORT
) {
    private val appContext = context.applicationContext
    private val registry = PeerRegistry(deviceId)
    private var receiverJob: Job? = null
    private var senderJob: Job? = null
    @Volatile private var socket: DatagramSocket? = null
    private var multicastLock: WifiManager.MulticastLock? = null
    @Volatile private var devicesCallback: ((List<DiscoveredDevice>) -> Unit)? = null

    fun start(
        scope: CoroutineScope,
        onDevices: (List<DiscoveredDevice>) -> Unit,
        onError: (String) -> Unit
    ) {
        devicesCallback = onDevices
        if (receiverJob?.isActive == true) return
        val wifiManager = appContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
        multicastLock = wifiManager.createMulticastLock("transfer-discovery").apply {
            setReferenceCounted(false)
            acquire()
        }
        val datagramSocket = runCatching {
            DatagramSocket(null).apply {
                reuseAddress = true
                broadcast = true
                soTimeout = RECEIVE_TIMEOUT_MILLIS
                bind(InetSocketAddress(DISCOVERY_PORT))
            }
        }.getOrElse {
            releaseLock()
            onError("无法启动局域网发现：${it.message.orEmpty()}")
            return
        }
        socket = datagramSocket
        receiverJob = scope.launch(Dispatchers.IO) {
            val buffer = ByteArray(DiscoveryPacketCodec.MAX_PACKET_BYTES)
            while (isActive) {
                try {
                    val packet = DatagramPacket(buffer, buffer.size)
                    datagramSocket.receive(packet)
                    val discovery = DiscoveryPacketCodec.decode(packet.data, packet.length)
                    if (discovery != null && registry.update(discovery, packet.address, now())) {
                        onDevices(registry.snapshot())
                    }
                } catch (_: SocketTimeoutException) {
                    if (registry.removeExpired(now())) onDevices(registry.snapshot())
                } catch (error: Exception) {
                    if (isActive && !datagramSocket.isClosed) {
                        onError("设备发现监听失败：${error.message.orEmpty()}")
                    }
                }
            }
        }
        senderJob = scope.launch(Dispatchers.IO) {
            val payload = DiscoveryPacketCodec.encode(deviceId, deviceName, tcpPort)
            while (isActive) {
                broadcastAddresses().forEach { address ->
                    runCatching {
                        datagramSocket.send(DatagramPacket(payload, payload.size, address, DISCOVERY_PORT))
                    }
                }
                delay(BROADCAST_INTERVAL_MILLIS)
            }
        }
    }

    fun addQrPeer(device: DiscoveredDevice): Boolean {
        val added = registry.addSessionPeer(device)
        if (added) devicesCallback?.invoke(registry.snapshot())
        return added
    }

    fun stop() {
        receiverJob?.cancel()
        senderJob?.cancel()
        receiverJob = null
        senderJob = null
        socket?.close()
        socket = null
        devicesCallback = null
        releaseLock()
    }

    private fun releaseLock() {
        multicastLock?.let { if (it.isHeld) it.release() }
        multicastLock = null
    }

    private fun broadcastAddresses(): Set<InetAddress> {
        val addresses = linkedSetOf<InetAddress>()
        runCatching {
            Collections.list(NetworkInterface.getNetworkInterfaces())
                .filter { it.isUp && !it.isLoopback }
                .flatMap { it.interfaceAddresses }
                .filter { it.address is Inet4Address && it.broadcast != null }
                .mapTo(addresses) { it.broadcast }
        }
        addresses += InetAddress.getByName("255.255.255.255")
        return addresses
    }

    private fun now(): Long = System.currentTimeMillis()

    companion object {
        const val DISCOVERY_PORT = 42042
        private const val BROADCAST_INTERVAL_MILLIS = 2_000L
        private const val RECEIVE_TIMEOUT_MILLIS = 1_000
    }
}
