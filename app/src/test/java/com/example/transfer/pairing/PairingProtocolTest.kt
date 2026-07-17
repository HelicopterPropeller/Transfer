package com.example.transfer.pairing

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import com.example.transfer.protocol.ProtocolException
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class PairingProtocolTest {
    @Test
    fun `request and accepted response round trip`() {
        val request = PairingRequest("token", "sender-id", "Sender")
        assertEquals(request, readRequest(write { PairingProtocol.writeRequest(it, request) }))

        val response = PairingResponse.accepted("receiver-id", "Receiver", 42043)
        assertEquals(response, readResponse(write { PairingProtocol.writeResponse(it, response) }))
    }

    @Test
    fun `rejected and protocol error responses have no identity`() {
        listOf(PairingStatus.REJECTED, PairingStatus.PROTOCOL_ERROR).forEach { status ->
            val decoded = readResponse(write { PairingProtocol.writeResponse(it, PairingResponse(status)) })
            assertEquals(PairingResponse(status), decoded)
        }
    }

    @Test
    fun `codec rejects oversized text invalid status and invalid port`() {
        assertThrows(IllegalArgumentException::class.java) {
            PairingProtocol.writeRequest(
                DataOutputStream(ByteArrayOutputStream()),
                PairingRequest("t".repeat(129), "id", "name")
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            PairingResponse.accepted("id", "name", 0)
        }
        assertThrows(ProtocolException::class.java) {
            readResponse(byteArrayOf(99.toByte()))
        }
    }

    private fun write(block: (DataOutputStream) -> Unit): ByteArray =
        ByteArrayOutputStream().also { bytes ->
            DataOutputStream(bytes).use(block)
        }.toByteArray()

    private fun readRequest(bytes: ByteArray) =
        PairingProtocol.readRequest(DataInputStream(ByteArrayInputStream(bytes)))

    private fun readResponse(bytes: ByteArray) =
        PairingProtocol.readResponse(DataInputStream(ByteArrayInputStream(bytes)))
}
