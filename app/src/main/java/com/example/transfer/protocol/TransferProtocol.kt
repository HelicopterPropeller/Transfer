package com.example.transfer.protocol

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

data class TransferHeader(
    val fileName: String,
    val mimeType: String,
    val fileSize: Long
)

class ProtocolException(message: String) : IOException(message)

object TransferProtocol {
    const val MAX_FILE_SIZE = 10L * 1024 * 1024 * 1024
    const val SUCCESS: Int = 0
    const val FAILURE: Int = 1
    private const val VERSION = 1
    private const val MAX_FILE_NAME_BYTES = 1024
    private const val MAX_MIME_BYTES = 256
    private val MAGIC = byteArrayOf('L'.code.toByte(), 'T'.code.toByte(), 'F'.code.toByte(), '1'.code.toByte())

    fun writeHeader(output: DataOutputStream, header: TransferHeader) {
        validateFileSize(header.fileSize)
        val fileName = header.fileName.toByteArray(Charsets.UTF_8)
        val mime = header.mimeType.toByteArray(Charsets.UTF_8)
        validateMetadata(fileName, MAX_FILE_NAME_BYTES, "文件名")
        validateMetadata(mime, MAX_MIME_BYTES, "MIME")

        output.write(MAGIC)
        output.writeByte(VERSION)
        output.writeInt(fileName.size)
        output.write(fileName)
        output.writeInt(mime.size)
        output.write(mime)
        output.writeLong(header.fileSize)
    }

    fun readHeader(input: DataInputStream): TransferHeader {
        val magic = ByteArray(MAGIC.size)
        input.readFully(magic)
        if (!magic.contentEquals(MAGIC)) throw ProtocolException("无效的传输协议标识")
        if (input.readUnsignedByte() != VERSION) throw ProtocolException("不支持的传输协议版本")
        val fileName = readText(input, MAX_FILE_NAME_BYTES, "文件名")
        val mime = readText(input, MAX_MIME_BYTES, "MIME")
        val fileSize = input.readLong()
        validateFileSize(fileSize)
        return TransferHeader(fileName, mime, fileSize)
    }

    private fun readText(input: DataInputStream, maximum: Int, label: String): String {
        val length = input.readInt()
        if (length !in 1..maximum) throw ProtocolException("$label 长度非法")
        return ByteArray(length).also(input::readFully).toString(Charsets.UTF_8)
    }

    private fun validateMetadata(bytes: ByteArray, maximum: Int, label: String) {
        if (bytes.isEmpty() || bytes.size > maximum) throw ProtocolException("$label 长度非法")
    }

    private fun validateFileSize(size: Long) {
        if (size !in 0..MAX_FILE_SIZE) throw ProtocolException("文件大小非法")
    }
}
