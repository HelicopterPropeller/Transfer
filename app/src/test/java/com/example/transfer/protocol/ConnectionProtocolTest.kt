package com.example.transfer.protocol

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class ConnectionProtocolTest {
    @Test
    fun `v4 preamble round trips every reserved connection kind`() {
        ConnectionKind.entries.forEach { kind ->
            val bytes = ByteArrayOutputStream().also {
                ConnectionProtocol.writePreamble(DataOutputStream(it), kind)
            }.toByteArray()

            assertEquals("LTF4", bytes.take(4).map(Byte::toInt).map(Int::toChar).joinToString(""))
            assertEquals(4, bytes[4].toInt())
            assertEquals(kind, ConnectionProtocol.readPreamble(DataInputStream(ByteArrayInputStream(bytes))))
        }
    }

    @Test
    fun `v3 preamble is rejected`() {
        val bytes = byteArrayOf(
            'L'.code.toByte(),
            'T'.code.toByte(),
            'F'.code.toByte(),
            '3'.code.toByte(),
            3,
            2
        )

        assertThrows(ProtocolException::class.java) {
            ConnectionProtocol.readPreamble(DataInputStream(ByteArrayInputStream(bytes)))
        }
    }

    @Test
    fun `unknown connection kind is rejected`() {
        val bytes = byteArrayOf(
            'L'.code.toByte(),
            'T'.code.toByte(),
            'F'.code.toByte(),
            '4'.code.toByte(),
            4,
            99
        )

        assertThrows(ProtocolException::class.java) {
            ConnectionProtocol.readPreamble(DataInputStream(ByteArrayInputStream(bytes)))
        }
    }
}
