package com.example.transfer.protocol

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.util.concurrent.CancellationException

class DigestChainTest {
    @Test
    fun `chain changes with chunk order index and length`() {
        val a = ChunkCodec.sha256(byteArrayOf(1, 2))
        val b = ChunkCodec.sha256(byteArrayOf(3))
        val first = DigestChain.next(DigestChain.initial(), 0, 2, a)

        assertFalse(first.contentEquals(DigestChain.next(DigestChain.initial(), 1, 2, a)))
        assertFalse(first.contentEquals(DigestChain.next(DigestChain.initial(), 0, 1, a)))
        assertFalse(first.contentEquals(DigestChain.next(DigestChain.initial(), 0, 2, b)))
    }

    @Test
    fun `chain uses canonical big endian input`() {
        val previous = ByteArray(ChunkCodec.DIGEST_SIZE) { it.toByte() }
        val chunkHash = ChunkCodec.sha256(byteArrayOf(9, 8, 7))
        val canonicalInput = previous + ByteBuffer.allocate(8)
            .order(ByteOrder.BIG_ENDIAN)
            .putInt(23)
            .putInt(3)
            .array() + chunkHash

        assertArrayEquals(
            ChunkCodec.sha256(canonicalInput),
            DigestChain.next(previous, 23, 3, chunkHash)
        )
    }

    @Test
    fun `prefix scanner returns next index chain last hash and whole prefix digest`() {
        val bytes = byteArrayOf(1, 2, 3, 4, 5)
        val firstHash = ChunkCodec.sha256(byteArrayOf(1, 2))
        val secondHash = ChunkCodec.sha256(byteArrayOf(3, 4))
        val lastHash = ChunkCodec.sha256(byteArrayOf(5))
        val expectedChain = DigestChain.next(
            DigestChain.next(
                DigestChain.next(DigestChain.initial(), 0, 2, firstHash),
                1,
                2,
                secondHash
            ),
            2,
            1,
            lastHash
        )

        val result = PrefixDigestScanner.scan(ByteArrayInputStream(bytes), bytes.size.toLong(), 2)

        assertEquals(3, result.nextChunkIndex)
        assertEquals(bytes.size.toLong(), result.scannedBytes)
        assertArrayEquals(expectedChain, result.chainDigest)
        assertArrayEquals(lastHash, result.lastChunkHash)
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(bytes), result.wholeDigest.digest())
    }

    @Test
    fun `empty prefix returns zero digest state`() {
        val result = PrefixDigestScanner.scan(ByteArrayInputStream(byteArrayOf(1)), 0, 2)

        assertEquals(0, result.nextChunkIndex)
        assertEquals(0L, result.scannedBytes)
        assertArrayEquals(ByteArray(ChunkCodec.DIGEST_SIZE), result.chainDigest)
        assertArrayEquals(ByteArray(ChunkCodec.DIGEST_SIZE), result.lastChunkHash)
        assertArrayEquals(ChunkCodec.sha256(byteArrayOf()), result.wholeDigest.digest())
    }

    @Test
    fun `whole digest can continue after prefix scan`() {
        val bytes = byteArrayOf(1, 2, 3, 4, 5)
        val result = PrefixDigestScanner.scan(ByteArrayInputStream(bytes), 3, 2)

        result.wholeDigest.update(bytes, 3, 2)

        assertArrayEquals(ChunkCodec.sha256(bytes), result.wholeDigest.digest())
    }

    @Test
    fun `prefix scanner rejects early end of file`() {
        val error = assertThrows(EOFException::class.java) {
            PrefixDigestScanner.scan(ByteArrayInputStream(byteArrayOf(1, 2, 3)), 4, 2)
        }

        assertEquals("File ended before committed prefix", error.message)
    }

    @Test
    fun `prefix scanner cooperatively checks cancellation between chunks`() {
        var checks = 0

        assertThrows(CancellationException::class.java) {
            PrefixDigestScanner.scan(ByteArrayInputStream(ByteArray(6)), 6, 2) {
                checks++
                if (checks == 3) throw CancellationException("stopped")
            }
        }

        assertEquals(3, checks)
    }
}
