package com.example.transfer.protocol

import java.io.EOFException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest

data class PrefixDigest(
    val scannedBytes: Long,
    val nextChunkIndex: Int,
    val chainDigest: ByteArray,
    val lastChunkHash: ByteArray,
    val wholeDigest: MessageDigest
)

object DigestChain {
    fun initial(): ByteArray = ByteArray(ChunkCodec.DIGEST_SIZE)

    fun next(previous: ByteArray, index: Int, length: Int, chunkHash: ByteArray): ByteArray {
        require(previous.size == ChunkCodec.DIGEST_SIZE)
        require(index >= 0 && length in 1..TransferProtocol.CHUNK_SIZE)
        require(chunkHash.size == ChunkCodec.DIGEST_SIZE)

        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(previous)
        digest.update(
            ByteBuffer.allocate(Int.SIZE_BYTES * 2)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(index)
                .putInt(length)
                .array()
        )
        digest.update(chunkHash)
        return digest.digest()
    }
}

object PrefixDigestScanner {
    fun scan(
        input: InputStream,
        bytes: Long,
        chunkSize: Int = TransferProtocol.CHUNK_SIZE
    ): PrefixDigest {
        require(bytes >= 0 && chunkSize > 0)

        var remaining = bytes
        var index = 0
        var chain = DigestChain.initial()
        var last = ByteArray(ChunkCodec.DIGEST_SIZE)
        val whole = MessageDigest.getInstance("SHA-256")
        val buffer = ByteArray(chunkSize)

        while (remaining > 0) {
            val length = minOf(chunkSize.toLong(), remaining).toInt()
            readFully(input, buffer, length)
            whole.update(buffer, 0, length)
            last = ChunkCodec.sha256(buffer.copyOf(length))
            chain = DigestChain.next(chain, index, length, last)
            remaining -= length
            index++
        }

        return PrefixDigest(bytes, index, chain, last, whole)
    }

    private fun readFully(input: InputStream, buffer: ByteArray, length: Int) {
        var offset = 0
        while (offset < length) {
            val read = input.read(buffer, offset, length - offset)
            if (read < 0) throw EOFException("File ended before committed prefix")
            if (read > 0) offset += read
        }
    }
}
