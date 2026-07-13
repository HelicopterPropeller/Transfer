package com.example.transfer.protocol

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.security.MessageDigest

class ChunkCodecTest {
    @Test
    fun `chunk round trips with sha256`() {
        val data = "verified chunk".toByteArray()
        val frame = ChunkCodec.create(3, data)
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(data), frame.digest)
        val bytes = ByteArrayOutputStream().also { ChunkCodec.write(DataOutputStream(it), frame) }.toByteArray()
        val actual = ChunkCodec.read(DataInputStream(ByteArrayInputStream(bytes)), 3, data.size)
        assertEquals(3, actual.index)
        assertArrayEquals(data, actual.data)
        assertArrayEquals(frame.digest, actual.digest)
    }

    @Test
    fun `unexpected index and length are rejected`() {
        val frame = ChunkCodec.create(0, byteArrayOf(1, 2, 3))
        val bytes = ByteArrayOutputStream().also { ChunkCodec.write(DataOutputStream(it), frame) }.toByteArray()
        assertThrows(ProtocolException::class.java) {
            ChunkCodec.read(DataInputStream(ByteArrayInputStream(bytes)), 1, 3)
        }
        assertThrows(ProtocolException::class.java) {
            ChunkCodec.create(0, ByteArray(TransferProtocol.CHUNK_SIZE + 1))
        }
    }
}
