package com.example.transfer.pairing

import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.ProtocolException
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class PairingClientTest {
    @Test
    fun `client sends local identity and accepts matching receiver identity`() {
        withPairingServer(PairingResponse.accepted("receiver", "Receiver", 42043)) { server, request ->
            val client = redirectedClient(server)
            val peer = client.connect(payload(), "sender", "Sender")

            assertEquals(PairingRequest("token", "sender", "Sender"), request.get(2, TimeUnit.SECONDS))
            assertEquals("receiver", peer.id)
            assertEquals("192.168.1.5", peer.address.hostAddress)
        }
    }

    @Test
    fun `client rejects failed status and mismatched qr identity`() {
        withPairingServer(PairingResponse(PairingStatus.REJECTED)) { server, _ ->
            assertThrows(ProtocolException::class.java) {
                redirectedClient(server).connect(payload(), "sender", "Sender")
            }
        }
        withPairingServer(PairingResponse.accepted("impostor", "Receiver", 42043)) { server, _ ->
            assertThrows(ProtocolException::class.java) {
                redirectedClient(server).connect(payload(), "sender", "Sender")
            }
        }
    }

    private fun redirectedClient(server: ServerSocket) = PairingClient { _, _, _ ->
        Socket(InetAddress.getLoopbackAddress(), server.localPort)
    }

    private fun payload() = PairingPayload(
        4, "receiver", "Receiver", "192.168.1.5", 42043, "token", Long.MAX_VALUE
    )

    private fun withPairingServer(
        response: PairingResponse,
        action: (ServerSocket, CompletableFuture<PairingRequest>) -> Unit
    ) {
        ServerSocket(0).use { server ->
            val request = CompletableFuture<PairingRequest>()
            val worker = Thread {
                server.accept().use { socket ->
                    val input = DataInputStream(socket.getInputStream())
                    val output = DataOutputStream(socket.getOutputStream())
                    check(ConnectionProtocol.readPreamble(input) == ConnectionKind.PAIR_REQUEST)
                    request.complete(PairingProtocol.readRequest(input))
                    PairingProtocol.writeResponse(output, response)
                    output.flush()
                }
            }.apply { start() }
            try {
                action(server, request)
            } finally {
                worker.join(2_000)
            }
        }
    }
}
