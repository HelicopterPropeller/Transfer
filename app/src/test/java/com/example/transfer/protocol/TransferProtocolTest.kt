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
    fun `header round trips utf8 metadata`() {
        val expected = TransferHeader("照片 01.jpg", "image/jpeg", 987654321L)
        val bytes = ByteArrayOutputStream().also {
            TransferProtocol.writeHeader(DataOutputStream(it), expected)
        }.toByteArray()

        val actual = TransferProtocol.readHeader(DataInputStream(ByteArrayInputStream(bytes)))

        assertEquals(expected, actual)
    }

    @Test
    fun `invalid magic is rejected`() {
        val bytes = validHeaderBytes().also { it[0] = 'X'.code.toByte() }
        assertThrows(ProtocolException::class.java) {
            TransferProtocol.readHeader(DataInputStream(ByteArrayInputStream(bytes)))
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

    @Test
    fun `metadata limit counts utf8 bytes`() {
        val tooLong = "文".repeat(342)
        assertThrows(ProtocolException::class.java) {
            TransferProtocol.writeHeader(
                DataOutputStream(ByteArrayOutputStream()),
                TransferHeader(tooLong, "application/octet-stream", 1)
            )
        }
    }

    private fun validHeaderBytes(): ByteArray = ByteArrayOutputStream().also {
        TransferProtocol.writeHeader(
            DataOutputStream(it),
            TransferHeader("a.txt", "text/plain", 1)
        )
    }.toByteArray()
}
