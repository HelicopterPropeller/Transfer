package com.example.transfer.pairing

import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.ProtocolException
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Inet4Address
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicReference

data class PairedPeer(
    val id: String,
    val name: String,
    val address: Inet4Address,
    val port: Int
)

class PairingClient(
    private val connector: (Inet4Address, Int, Int) -> Socket = { address, port, timeout ->
        Socket().apply { connect(InetSocketAddress(address, port), timeout) }
    }
) {
    private val activeSocket = AtomicReference<Socket?>()

    fun connect(
        payload: PairingPayload,
        localDeviceId: String,
        localDeviceName: String
    ): PairedPeer {
        val address = canonicalPrivateIpv4(payload.ip)
            ?: throw PairingPayloadException("Address is not a private IPv4")
        val socket = connector(address, payload.port, CONNECT_TIMEOUT_MILLIS)
        if (!activeSocket.compareAndSet(null, socket)) {
            socket.close()
            throw ProtocolException("Another pairing request is active")
        }
        try {
            socket.use {
            socket.soTimeout = READ_TIMEOUT_MILLIS
            val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
            ConnectionProtocol.writePreamble(output, ConnectionKind.PAIR_REQUEST)
            PairingProtocol.writeRequest(
                output,
                PairingRequest(payload.token, localDeviceId, localDeviceName)
            )
            output.flush()
            val response = PairingProtocol.readResponse(input)
            if (response.status != PairingStatus.ACCEPTED) {
                throw ProtocolException("Pairing request was rejected")
            }
            if (response.receiverDeviceId != payload.deviceId || response.tcpPort != payload.port) {
                throw ProtocolException("Pairing identity does not match QR payload")
            }
            return PairedPeer(
                id = requireNotNull(response.receiverDeviceId),
                name = requireNotNull(response.receiverDeviceName),
                address = address,
                port = requireNotNull(response.tcpPort)
            )
            }
        } finally {
            activeSocket.compareAndSet(socket, null)
        }
    }

    fun cancelActive() {
        activeSocket.getAndSet(null)?.let { runCatching { it.close() } }
    }

    companion object {
        const val CONNECT_TIMEOUT_MILLIS = 5_000
        const val READ_TIMEOUT_MILLIS = 15_000
    }
}
