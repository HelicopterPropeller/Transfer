package com.example.transfer.protocol

import java.io.DataInputStream
import java.io.DataOutputStream

enum class ConnectionKind(val code: Int) {
    RESUME_QUERY(1),
    TRANSFER_START(2),
    PAIR_REQUEST(3)
}

object ConnectionProtocol {
    const val VERSION = 4
    private val MAGIC = byteArrayOf(
        'L'.code.toByte(),
        'T'.code.toByte(),
        'F'.code.toByte(),
        '4'.code.toByte()
    )

    fun writePreamble(output: DataOutputStream, kind: ConnectionKind) {
        output.write(MAGIC)
        output.writeByte(VERSION)
        output.writeByte(kind.code)
    }

    fun readPreamble(input: DataInputStream): ConnectionKind {
        val magic = ByteArray(MAGIC.size).also(input::readFully)
        if (!magic.contentEquals(MAGIC)) throw ProtocolException("Invalid transfer magic")
        if (input.readUnsignedByte() != VERSION) throw ProtocolException("Unsupported protocol version")
        val code = input.readUnsignedByte()
        return ConnectionKind.entries.firstOrNull { it.code == code }
            ?: throw ProtocolException("Unknown connection kind")
    }
}

internal fun DataOutputStream.writeBoundedText(value: String, maximum: Int, label: String) {
    val bytes = value.toByteArray(Charsets.UTF_8)
    if (bytes.isEmpty() || bytes.size > maximum) throw ProtocolException("Invalid $label length")
    writeInt(bytes.size)
    write(bytes)
}

internal fun DataInputStream.readBoundedText(maximum: Int, label: String): String {
    val length = readInt()
    if (length !in 1..maximum) throw ProtocolException("Invalid $label length")
    return ByteArray(length).also(::readFully).toString(Charsets.UTF_8)
}
