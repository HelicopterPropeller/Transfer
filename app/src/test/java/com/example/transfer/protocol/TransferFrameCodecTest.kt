package com.example.transfer.protocol

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class TransferFrameCodecTest {
    @Test
    fun `frame types round trip and unknown code is rejected`() {
        TransferFrameType.entries.forEach { expected ->
            val bytes = ByteArrayOutputStream().also {
                TransferFrameCodec.writeType(DataOutputStream(it), expected)
            }.toByteArray()
            assertEquals(expected, TransferFrameCodec.readType(DataInputStream(ByteArrayInputStream(bytes))))
        }
        assertThrows(ProtocolException::class.java) {
            TransferFrameCodec.readType(DataInputStream(ByteArrayInputStream(byteArrayOf(99))))
        }
    }
}
