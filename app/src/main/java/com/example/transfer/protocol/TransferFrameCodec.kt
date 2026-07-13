package com.example.transfer.protocol

import java.io.DataInputStream
import java.io.DataOutputStream

enum class TransferFrameType(val code: Int) { CHUNK(0), PAUSE(1), RESUME(2) }

object TransferFrameCodec {
    fun writeType(output: DataOutputStream, type: TransferFrameType) = output.writeByte(type.code)

    fun readType(input: DataInputStream): TransferFrameType {
        val code = input.readUnsignedByte()
        return TransferFrameType.entries.firstOrNull { it.code == code }
            ?: throw ProtocolException("Unknown frame type")
    }
}
