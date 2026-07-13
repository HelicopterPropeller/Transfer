package com.example.transfer.protocol

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class TransferProtocolTest {
    @Test
    fun `LTF2 header round trips with fixed chunk size`() {
        val expected = TransferHeader("photo.jpg", "image/jpeg", 987654321L)
        val bytes = ByteArrayOutputStream().also {
            TransferProtocol.writeHeader(DataOutputStream(it), expected)
        }.toByteArray()

        assertEquals("LTF2", bytes.take(4).map(Byte::toInt).map(Int::toChar).joinToString(""))
        assertEquals(2, bytes[4].toInt())
        assertEquals(expected, TransferProtocol.readHeader(DataInputStream(ByteArrayInputStream(bytes))))
        assertEquals(1_048_576, expected.chunkSize)
    }

    @Test
    fun `invalid magic and chunk size are rejected`() {
        val invalidMagic = validHeaderBytes().also { it[0] = 'X'.code.toByte() }
        assertThrows(ProtocolException::class.java) {
            TransferProtocol.readHeader(DataInputStream(ByteArrayInputStream(invalidMagic)))
        }
        assertThrows(ProtocolException::class.java) {
            TransferProtocol.writeHeader(
                DataOutputStream(ByteArrayOutputStream()),
                TransferHeader("a", "x", 1, 1024)
            )
        }
    }

    @Test
    fun `file larger than ten gib is rejected`() {
        assertThrows(ProtocolException::class.java) {
            TransferProtocol.writeHeader(
                DataOutputStream(ByteArrayOutputStream()),
                TransferHeader("large.bin", "application/octet-stream", TransferProtocol.MAX_FILE_SIZE + 1)
            )
        }
    }

    private fun validHeaderBytes(): ByteArray = ByteArrayOutputStream().also {
        TransferProtocol.writeHeader(DataOutputStream(it), TransferHeader("a.txt", "text/plain", 1))
    }.toByteArray()
}
