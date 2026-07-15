package com.example.transfer.protocol

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

data class TransferHeader(
    val fileName: String,
    val mimeType: String,
    val fileSize: Long,
    val chunkSize: Int = TransferProtocol.CHUNK_SIZE
)

class ProtocolException(message: String) : IOException(message)

object TransferProtocol {
    const val MAX_FILE_SIZE = 10L * 1024 * 1024 * 1024
    const val CHUNK_SIZE = 1_048_576
    const val ACK = 0
    const val NACK = 1
    const val FATAL = 2
    const val CONTROL_ACK = 3
    const val COMPLETE = 0
    const val FAILED = 1
    const val SUCCESS = COMPLETE
    const val FAILURE = FAILED
    private const val VERSION = 3
    private val MAGIC = byteArrayOf('L'.code.toByte(), 'T'.code.toByte(), 'F'.code.toByte(), '3'.code.toByte())

    fun writeHeader(output: DataOutputStream, header: TransferHeader) {
        validateFileSize(header.fileSize)
        if (header.chunkSize != CHUNK_SIZE) throw ProtocolException("Invalid chunk size")
        output.write(MAGIC)
        output.writeByte(VERSION)
        output.writeBoundedText(header.fileName, ResumeProtocol.MAX_FILE_NAME_BYTES, "file name")
        output.writeBoundedText(header.mimeType, ResumeProtocol.MAX_MIME_BYTES, "MIME")
        output.writeLong(header.fileSize)
        output.writeInt(header.chunkSize)
    }

    fun readHeader(input: DataInputStream): TransferHeader {
        val magic = ByteArray(MAGIC.size)
        input.readFully(magic)
        if (!magic.contentEquals(MAGIC)) throw ProtocolException("Invalid transfer magic")
        if (input.readUnsignedByte() != VERSION) throw ProtocolException("Unsupported protocol version")
        val fileName = input.readBoundedText(ResumeProtocol.MAX_FILE_NAME_BYTES, "file name")
        val mime = input.readBoundedText(ResumeProtocol.MAX_MIME_BYTES, "MIME")
        val fileSize = input.readLong().also(::validateFileSize)
        val chunkSize = input.readInt()
        if (chunkSize != CHUNK_SIZE) throw ProtocolException("Invalid chunk size")
        return TransferHeader(fileName, mime, fileSize, chunkSize)
    }

    private fun validateFileSize(size: Long) {
        if (size !in 0..MAX_FILE_SIZE) throw ProtocolException("Invalid file size")
    }
}
