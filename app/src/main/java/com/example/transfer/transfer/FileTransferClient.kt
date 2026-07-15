package com.example.transfer.transfer

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.PrefixDigestScanner
import com.example.transfer.protocol.ResumeProtocol
import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferFrameCodec
import com.example.transfer.protocol.TransferFrameType
import com.example.transfer.protocol.TransferHeader
import com.example.transfer.protocol.TransferOffer
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.resume.PreparedTransfer
import com.example.transfer.resume.ResumeValidationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.InputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.security.MessageDigest

class FileTransferClient {
    @Volatile private var activeSocket: Socket? = null
    @Volatile private var activePauseController: TransferPauseController? = null

    suspend fun queryResume(
        host: InetAddress,
        port: Int,
        offer: TransferOffer
    ): ResumeStatus = withContext(Dispatchers.IO) {
        val socket = Socket()
        activeSocket = socket
        try {
            socket.use {
                socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS)
                socket.soTimeout = SOCKET_TIMEOUT_MILLIS
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                ConnectionProtocol.writePreamble(output, ConnectionKind.RESUME_QUERY)
                ResumeProtocol.writeOffer(output, offer)
                output.flush()
                ResumeProtocol.readStatus(input, offer)
            }
        } finally {
            if (activeSocket === socket) activeSocket = null
        }
    }

    suspend fun sendPrepared(
        host: InetAddress,
        port: Int,
        prepared: PreparedTransfer,
        mode: TransferStartMode,
        expectedStatus: ResumeStatus,
        pauseController: TransferPauseController,
        onPauseState: (TransferPauseState) -> Unit,
        onProgress: (FileTransferProgress) -> Unit
    ): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            prepared.source.openStream().use { fileInput ->
                val start = prepareStream(fileInput, prepared.offer, mode, expectedStatus)
                if (mode == TransferStartMode.RESUME) {
                    onProgress(FileTransferProgress(start.confirmedBytes, prepared.offer.fileSize))
                }

                val socket = Socket()
                activeSocket = socket
                activePauseController = pauseController
                try {
                    socket.use {
                        socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS)
                        socket.soTimeout = SOCKET_TIMEOUT_MILLIS
                        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                        val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                        ConnectionProtocol.writePreamble(output, ConnectionKind.TRANSFER_START)
                        ResumeProtocol.writeOffer(output, prepared.offer)
                        ResumeProtocol.writeStartMode(output, mode)
                        ResumeProtocol.writeStatus(output, expectedStatus)
                        output.flush()

                        sendPreparedChunks(
                            fileInput = fileInput,
                            total = prepared.offer.fileSize,
                            initialConfirmed = start.confirmedBytes,
                            initialIndex = start.nextChunkIndex,
                            wholeDigest = start.wholeDigest,
                            input = input,
                            output = output,
                            pauseController = pauseController,
                            onPauseState = onPauseState,
                            onProgress = onProgress
                        )
                        output.writeByte(TransferProtocol.COMPLETE)
                        output.write(start.wholeDigest.digest())
                        output.flush()
                        if (input.readUnsignedByte() != TransferProtocol.SUCCESS) {
                            error("Receiver could not finalize the file")
                        }
                        if (prepared.offer.fileSize == 0L && mode != TransferStartMode.RESUME) {
                            onProgress(FileTransferProgress(0, 0))
                        }
                    }
                } finally {
                    if (activeSocket === socket) activeSocket = null
                    if (activePauseController === pauseController) activePauseController = null
                }
            }
        }
    }

    suspend fun send(
        host: InetAddress,
        port: Int,
        source: SendFileSource,
        pauseController: TransferPauseController,
        onPauseState: (TransferPauseState) -> Unit,
        onProgress: (FileTransferProgress) -> Unit
    ): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val socket = Socket()
            activeSocket = socket
            activePauseController = pauseController
            try {
                socket.use {
                    socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS)
                    socket.soTimeout = SOCKET_TIMEOUT_MILLIS
                    val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                    val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                    TransferProtocol.writeHeader(output, TransferHeader(source.displayName, source.mimeType, source.length))
                    output.flush()
                    source.openStream().use { fileInput ->
                        sendChunks(
                            fileInput,
                            source.length,
                            input,
                            output,
                            pauseController,
                            onPauseState,
                            onProgress
                        )
                    }
                    if (input.readUnsignedByte() != TransferProtocol.COMPLETE) {
                        error("Receiver could not finalize the file")
                    }
                    if (source.length == 0L) onProgress(FileTransferProgress(0, 0))
                }
            } finally {
                if (activeSocket === socket) activeSocket = null
                if (activePauseController === pauseController) activePauseController = null
            }
        }
    }

    suspend fun send(
        host: InetAddress,
        port: Int,
        source: SendFileSource,
        onProgress: (Int) -> Unit
    ): Result<Unit> = send(
        host,
        port,
        source,
        TransferPauseController(),
        {},
        { onProgress(it.percent) }
    )

    fun cancelActive() {
        activePauseController?.cancel()
        runCatching { activeSocket?.close() }
        activePauseController = null
        activeSocket = null
    }

    private fun sendChunks(
        fileInput: InputStream,
        total: Long,
        input: DataInputStream,
        output: DataOutputStream,
        pauseController: TransferPauseController,
        onPauseState: (TransferPauseState) -> Unit,
        onProgress: (FileTransferProgress) -> Unit
    ) {
        var confirmed = 0L
        var index = 0
        while (confirmed < total) {
            val length = minOf(TransferProtocol.CHUNK_SIZE.toLong(), total - confirmed).toInt()
            val frame = ChunkCodec.create(index, readExactly(fileInput, length))
            var acknowledged = false
            for (attempt in 1..MAX_ATTEMPTS) {
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, frame)
                output.flush()
                when (input.readUnsignedByte()) {
                    TransferProtocol.ACK -> { acknowledged = true; break }
                    TransferProtocol.NACK -> Unit
                    TransferProtocol.FATAL -> error("Receiver rejected the transfer")
                    else -> error("Unknown chunk response")
                }
            }
            if (!acknowledged) error("Chunk $index failed verification after $MAX_ATTEMPTS attempts")
            confirmed += length
            onProgress(FileTransferProgress(confirmed, total))
            if (confirmed < total) {
                pauseController.checkpoint(
                    sendPause = {
                        TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                        output.flush()
                        if (input.readUnsignedByte() != TransferProtocol.CONTROL_ACK) {
                            error("Pause was not acknowledged")
                        }
                    },
                    sendResume = {
                        TransferFrameCodec.writeType(output, TransferFrameType.RESUME)
                        output.flush()
                        if (input.readUnsignedByte() != TransferProtocol.CONTROL_ACK) {
                            error("Resume was not acknowledged")
                        }
                    },
                    onState = onPauseState
                )
            }
            index++
        }
    }

    private fun prepareStream(
        fileInput: InputStream,
        offer: TransferOffer,
        mode: TransferStartMode,
        expectedStatus: ResumeStatus
    ): PreparedStream {
        if (mode != TransferStartMode.RESUME) {
            return PreparedStream(0, 0, MessageDigest.getInstance("SHA-256"))
        }
        if (expectedStatus.state != ResumeState.AVAILABLE ||
            expectedStatus.confirmedBytes > offer.fileSize ||
            (expectedStatus.confirmedBytes != offer.fileSize &&
                expectedStatus.confirmedBytes % offer.chunkSize != 0L)
        ) {
            throw ResumeValidationException("Checkpoint is not resumable for this file")
        }
        val prefix = PrefixDigestScanner.scan(
            fileInput,
            expectedStatus.confirmedBytes,
            offer.chunkSize
        )
        if (prefix.nextChunkIndex != expectedStatus.nextChunkIndex ||
            !prefix.chainDigest.contentEquals(expectedStatus.chainDigest) ||
            !prefix.lastChunkHash.contentEquals(expectedStatus.lastChunkHash)
        ) {
            throw ResumeValidationException("Source prefix does not match receiver checkpoint")
        }
        return PreparedStream(prefix.scannedBytes, prefix.nextChunkIndex, prefix.wholeDigest)
    }

    private fun sendPreparedChunks(
        fileInput: InputStream,
        total: Long,
        initialConfirmed: Long,
        initialIndex: Int,
        wholeDigest: MessageDigest,
        input: DataInputStream,
        output: DataOutputStream,
        pauseController: TransferPauseController,
        onPauseState: (TransferPauseState) -> Unit,
        onProgress: (FileTransferProgress) -> Unit
    ) {
        var confirmed = initialConfirmed
        var index = initialIndex
        while (confirmed < total) {
            val length = minOf(TransferProtocol.CHUNK_SIZE.toLong(), total - confirmed).toInt()
            val data = readExactly(fileInput, length)
            wholeDigest.update(data)
            val frame = ChunkCodec.create(index, data)
            var acknowledged = false
            for (attempt in 1..MAX_ATTEMPTS) {
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, frame)
                output.flush()
                when (input.readUnsignedByte()) {
                    TransferProtocol.ACK -> {
                        acknowledged = true
                        break
                    }
                    TransferProtocol.NACK -> Unit
                    TransferProtocol.FATAL -> error("Receiver rejected the transfer")
                    else -> error("Unknown chunk response")
                }
            }
            if (!acknowledged) error("Chunk $index failed verification after $MAX_ATTEMPTS attempts")
            confirmed += length
            onProgress(FileTransferProgress(confirmed, total))
            if (confirmed < total) {
                pauseController.checkpoint(
                    sendPause = {
                        TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                        output.flush()
                        if (input.readUnsignedByte() != TransferProtocol.CONTROL_ACK) {
                            error("Pause was not acknowledged")
                        }
                    },
                    sendResume = {
                        TransferFrameCodec.writeType(output, TransferFrameType.RESUME)
                        output.flush()
                        if (input.readUnsignedByte() != TransferProtocol.CONTROL_ACK) {
                            error("Resume was not acknowledged")
                        }
                    },
                    onState = onPauseState
                )
            }
            index++
        }
    }

    private fun readExactly(input: InputStream, length: Int): ByteArray {
        val data = ByteArray(length)
        var offset = 0
        while (offset < length) {
            val read = input.read(data, offset, length - offset)
            if (read < 0) throw EOFException("Source file ended early")
            offset += read
        }
        return data
    }

    companion object {
        private const val CONNECT_TIMEOUT_MILLIS = 5_000
        private const val SOCKET_TIMEOUT_MILLIS = 15_000
        private const val MAX_ATTEMPTS = 3
    }

    private data class PreparedStream(
        val confirmedBytes: Long,
        val nextChunkIndex: Int,
        val wholeDigest: MessageDigest
    )
}
