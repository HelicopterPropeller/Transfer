package com.example.transfer.protocol

import java.io.DataInputStream
import java.io.DataOutputStream
import java.util.UUID

data class TransferOffer(
    val transferId: String,
    val senderDeviceId: String,
    val fileName: String,
    val mimeType: String,
    val fileSize: Long,
    val chunkSize: Int = TransferProtocol.CHUNK_SIZE
)

enum class ResumeState(val code: Int) {
    NONE(0),
    AVAILABLE(1),
    INVALID(2)
}

enum class TransferStartMode(val code: Int) {
    NEW(0),
    RESUME(1),
    RESTART(2)
}

data class ResumeStatus(
    val state: ResumeState,
    val confirmedBytes: Long = 0,
    val nextChunkIndex: Int = 0,
    val chainDigest: ByteArray = ByteArray(ChunkCodec.DIGEST_SIZE),
    val lastChunkHash: ByteArray = ByteArray(ChunkCodec.DIGEST_SIZE)
)

object ResumeProtocol {
    const val MAX_ID_BYTES = 64
    const val MAX_FILE_NAME_BYTES = 1024
    const val MAX_MIME_BYTES = 256
    const val STATUS_OK = 0
    const val STATUS_PROTOCOL_ERROR = 1

    fun writeOffer(output: DataOutputStream, offer: TransferOffer) {
        validateOffer(offer)
        output.writeBoundedText(offer.transferId, MAX_ID_BYTES, "transfer ID")
        output.writeBoundedText(offer.senderDeviceId, MAX_ID_BYTES, "sender device ID")
        output.writeBoundedText(offer.fileName, MAX_FILE_NAME_BYTES, "file name")
        output.writeBoundedText(offer.mimeType, MAX_MIME_BYTES, "MIME")
        output.writeLong(offer.fileSize)
        output.writeInt(offer.chunkSize)
    }

    fun readOffer(input: DataInputStream): TransferOffer = TransferOffer(
        transferId = input.readBoundedText(MAX_ID_BYTES, "transfer ID"),
        senderDeviceId = input.readBoundedText(MAX_ID_BYTES, "sender device ID"),
        fileName = input.readBoundedText(MAX_FILE_NAME_BYTES, "file name"),
        mimeType = input.readBoundedText(MAX_MIME_BYTES, "MIME"),
        fileSize = input.readLong(),
        chunkSize = input.readInt()
    ).also(::validateOffer)

    fun writeStatus(output: DataOutputStream, status: ResumeStatus) {
        validateStatus(status)
        output.writeByte(status.state.code)
        output.writeLong(status.confirmedBytes)
        output.writeInt(status.nextChunkIndex)
        output.write(status.chainDigest)
        output.write(status.lastChunkHash)
    }

    fun readStatus(input: DataInputStream): ResumeStatus {
        val stateCode = input.readUnsignedByte()
        val state = ResumeState.entries.firstOrNull { it.code == stateCode }
            ?: throw ProtocolException("Unknown resume state")
        return ResumeStatus(
            state = state,
            confirmedBytes = input.readLong(),
            nextChunkIndex = input.readInt(),
            chainDigest = ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully),
            lastChunkHash = ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully)
        ).also(::validateStatus)
    }

    fun readStatus(input: DataInputStream, offer: TransferOffer): ResumeStatus =
        readStatus(input).also { validateStatusForOffer(it, offer) }

    fun writeStartMode(output: DataOutputStream, mode: TransferStartMode) = output.writeByte(mode.code)

    fun readStartMode(input: DataInputStream): TransferStartMode {
        val code = input.readUnsignedByte()
        return TransferStartMode.entries.firstOrNull { it.code == code }
            ?: throw ProtocolException("Unknown transfer start mode")
    }

    private fun validateOffer(offer: TransferOffer) {
        runCatching { UUID.fromString(offer.transferId) }
            .getOrElse { throw ProtocolException("Invalid transfer ID") }
        if (offer.senderDeviceId.toByteArray(Charsets.UTF_8).size !in 1..MAX_ID_BYTES) {
            throw ProtocolException("Invalid sender device ID length")
        }
        if (offer.fileSize !in 0..TransferProtocol.MAX_FILE_SIZE) throw ProtocolException("Invalid file size")
        if (offer.chunkSize != TransferProtocol.CHUNK_SIZE) throw ProtocolException("Invalid chunk size")
    }

    private fun validateStatus(status: ResumeStatus) {
        if (status.confirmedBytes < 0 || status.nextChunkIndex < 0) {
            throw ProtocolException("Invalid resume offset")
        }
        val calculated = if (status.confirmedBytes == 0L) {
            0
        } else {
            ((status.confirmedBytes - 1) / TransferProtocol.CHUNK_SIZE + 1).toInt()
        }
        if (status.nextChunkIndex != calculated) throw ProtocolException("Invalid resume index")
        if (status.chainDigest.size != ChunkCodec.DIGEST_SIZE ||
            status.lastChunkHash.size != ChunkCodec.DIGEST_SIZE
        ) {
            throw ProtocolException("Invalid resume digest")
        }
    }

    private fun validateStatusForOffer(status: ResumeStatus, offer: TransferOffer) {
        if (status.confirmedBytes > offer.fileSize) throw ProtocolException("Invalid resume offset")
        if (status.confirmedBytes != offer.fileSize &&
            status.confirmedBytes % TransferProtocol.CHUNK_SIZE != 0L
        ) {
            throw ProtocolException("Invalid resume offset")
        }
    }
}
