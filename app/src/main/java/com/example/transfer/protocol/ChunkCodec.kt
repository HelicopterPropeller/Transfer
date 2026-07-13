package com.example.transfer.protocol

import java.io.DataInputStream
import java.io.DataOutputStream
import java.security.MessageDigest

data class ChunkFrame(
    val index: Int,
    val data: ByteArray,
    val digest: ByteArray
)

object ChunkCodec {
    const val DIGEST_SIZE = 32

    fun create(index: Int, data: ByteArray): ChunkFrame {
        if (index < 0 || data.size !in 1..TransferProtocol.CHUNK_SIZE) {
            throw ProtocolException("Invalid chunk")
        }
        val copy = data.copyOf()
        return ChunkFrame(index, copy, sha256(copy))
    }

    fun write(output: DataOutputStream, frame: ChunkFrame) {
        if (frame.index < 0 || frame.data.size !in 1..TransferProtocol.CHUNK_SIZE ||
            frame.digest.size != DIGEST_SIZE
        ) throw ProtocolException("Invalid chunk frame")
        output.writeInt(frame.index)
        output.writeInt(frame.data.size)
        output.write(frame.data)
        output.write(frame.digest)
    }

    fun read(input: DataInputStream, expectedIndex: Int, expectedLength: Int): ChunkFrame {
        val index = input.readInt()
        val length = input.readInt()
        if (index != expectedIndex || length != expectedLength || length !in 1..TransferProtocol.CHUNK_SIZE) {
            throw ProtocolException("Unexpected chunk")
        }
        val data = ByteArray(length).also(input::readFully)
        val digest = ByteArray(DIGEST_SIZE).also(input::readFully)
        return ChunkFrame(index, data, digest)
    }

    fun sha256(data: ByteArray): ByteArray = MessageDigest.getInstance("SHA-256").digest(data)
}
