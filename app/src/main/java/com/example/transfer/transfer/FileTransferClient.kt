package com.example.transfer.transfer

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.DigestChain
import com.example.transfer.protocol.PrefixDigest
import com.example.transfer.protocol.ProtocolException
import com.example.transfer.protocol.ResumeProtocol
import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferFrameCodec
import com.example.transfer.protocol.TransferFrameType
import com.example.transfer.protocol.TransferOffer
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.protocol.TransferStartResponse
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
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class FileTransferClient {
    private val nextOperationId = AtomicLong()
    private val activeOperation = AtomicReference<ActiveOperation?>()

    suspend fun queryResume(
        host: InetAddress,
        port: Int,
        offer: TransferOffer
    ): ResumeStatus = withContext(Dispatchers.IO) {
        val operation = registerOperation()
        try {
            val socket = Socket()
            attachSocket(operation.id, socket)
            socket.use {
                checkActive(operation.id)
                socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS)
                checkActive(operation.id)
                socket.soTimeout = queryReadTimeoutMillis(offer.fileSize)
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                checkActive(operation.id)
                ConnectionProtocol.writePreamble(output, ConnectionKind.RESUME_QUERY)
                ResumeProtocol.writeOffer(output, offer)
                output.flush()
                readResumeStatus(input, offer)
            }
        } finally {
            clearOperation(operation.id)
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
            val operation = registerOperation(pauseController)
            try {
                checkActive(operation.id)
                prepared.source.requireUnchanged()
                checkActive(operation.id)
                val openedSource = prepared.source.openStream()
                attachSource(operation.id, openedSource)
                openedSource.use { fileInput ->
                    val start = prepareStream(
                        fileInput, prepared.offer, mode, expectedStatus, operation.id
                    )
                    if (expectedStatus.state == ResumeState.COMPLETED) {
                        verifyCompletedDigest(start.wholeDigest, expectedStatus.finalDigest)
                        onProgress(FileTransferProgress(prepared.offer.fileSize, prepared.offer.fileSize))
                        return@use
                    }
                    checkActive(operation.id)
                    if (mode == TransferStartMode.RESUME) {
                        onProgress(FileTransferProgress(start.confirmedBytes, prepared.offer.fileSize))
                    }
                    checkActive(operation.id)

                    val socket = Socket()
                    attachSocket(operation.id, socket)
                    socket.use {
                        checkActive(operation.id)
                        socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS)
                        checkActive(operation.id)
                        socket.soTimeout = readyReadTimeoutMillis(expectedStatus.confirmedBytes)
                        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                        val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                        checkActive(operation.id)
                        ConnectionProtocol.writePreamble(output, ConnectionKind.TRANSFER_START)
                        ResumeProtocol.writeOffer(output, prepared.offer)
                        ResumeProtocol.writeStartMode(output, mode)
                        ResumeProtocol.writeStatus(output, expectedStatus)
                        output.flush()

                        val response = ResumeProtocol.readStartResponse(input)
                        when (response.response) {
                            TransferStartResponse.READY -> socket.soTimeout = SOCKET_TIMEOUT_MILLIS
                            TransferStartResponse.COMPLETED -> {
                                scanRemaining(
                                    fileInput,
                                    prepared.offer.fileSize - start.confirmedBytes,
                                    start.wholeDigest,
                                    operation.id
                                )
                                verifyCompletedDigest(start.wholeDigest, response.finalDigest)
                                onProgress(
                                    FileTransferProgress(
                                        prepared.offer.fileSize,
                                        prepared.offer.fileSize
                                    )
                                )
                                return@use
                            }
                            TransferStartResponse.FATAL -> error("Receiver rejected transfer start")
                        }

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
                            onProgress = onProgress,
                            operationId = operation.id
                        )
                        checkActive(operation.id)
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
                }
            } finally {
                clearOperation(operation.id)
            }
        }
    }

    fun cancelActive() {
        while (true) {
            val current = activeOperation.get() ?: return
            val cancelled = current.copy(cancelled = true)
            if (!activeOperation.compareAndSet(current, cancelled)) continue
            activeOperation.compareAndSet(cancelled, null)
            cancelled.pauseController?.cancel()
            runCatching { cancelled.source?.close() }
            runCatching { cancelled.socket?.close() }
            return
        }
    }

    private fun prepareStream(
        fileInput: InputStream,
        offer: TransferOffer,
        mode: TransferStartMode,
        expectedStatus: ResumeStatus,
        operationId: Long
    ): PreparedStream {
        checkActive(operationId)
        if (expectedStatus.state == ResumeState.COMPLETED) {
            if (expectedStatus.finalDigest?.size != ChunkCodec.DIGEST_SIZE) {
                throw ResumeValidationException("Completed receipt has no final digest")
            }
            val completed = scanPrefix(fileInput, offer.fileSize, offer.chunkSize, operationId)
            return PreparedStream(completed.scannedBytes, completed.nextChunkIndex, completed.wholeDigest)
        }
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
        val prefix = scanPrefix(
            fileInput,
            expectedStatus.confirmedBytes,
            offer.chunkSize,
            operationId
        )
        checkActive(operationId)
        if (prefix.nextChunkIndex != expectedStatus.nextChunkIndex ||
            !prefix.chainDigest.contentEquals(expectedStatus.chainDigest) ||
            !prefix.lastChunkHash.contentEquals(expectedStatus.lastChunkHash)
        ) {
            throw ResumeValidationException("Source prefix does not match receiver checkpoint")
        }
        return PreparedStream(prefix.scannedBytes, prefix.nextChunkIndex, prefix.wholeDigest)
    }

    private fun readResumeStatus(
        input: DataInputStream,
        offer: TransferOffer
    ): ResumeStatus {
        input.mark(1)
        val firstResponse = input.readUnsignedByte()
        input.reset()
        return try {
            ResumeProtocol.readStatus(input, offer)
        } catch (_: EOFException) {
            if (firstResponse == TransferProtocol.FATAL) {
                throw ProtocolException("对端未返回完整的 v4 响应，可能协议版本不兼容或连接已中断")
            }
            throw ProtocolException("Receiver closed the resume response early")
        }
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
        onProgress: (FileTransferProgress) -> Unit,
        operationId: Long
    ) {
        var confirmed = initialConfirmed
        var index = initialIndex
        while (confirmed < total) {
            checkActive(operationId)
            val length = minOf(TransferProtocol.CHUNK_SIZE.toLong(), total - confirmed).toInt()
            val data = readExactly(fileInput, length)
            checkActive(operationId)
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
            checkActive(operationId)
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

    private fun scanPrefix(
        input: InputStream,
        bytes: Long,
        chunkSize: Int,
        operationId: Long
    ): PrefixDigest {
        var remaining = bytes
        var index = 0
        var chain = DigestChain.initial()
        var last = ByteArray(ChunkCodec.DIGEST_SIZE)
        val whole = MessageDigest.getInstance("SHA-256")

        while (remaining > 0) {
            checkActive(operationId)
            val length = minOf(chunkSize.toLong(), remaining).toInt()
            val data = readExactly(input, length, operationId)
            checkActive(operationId)
            whole.update(data)
            last = ChunkCodec.sha256(data)
            chain = DigestChain.next(chain, index, length, last)
            remaining -= length
            index++
        }
        checkActive(operationId)
        return PrefixDigest(bytes, index, chain, last, whole)
    }

    private fun scanRemaining(
        input: InputStream,
        bytes: Long,
        wholeDigest: MessageDigest,
        operationId: Long
    ) {
        var remaining = bytes
        while (remaining > 0) {
            checkActive(operationId)
            val length = minOf(TransferProtocol.CHUNK_SIZE.toLong(), remaining).toInt()
            wholeDigest.update(readExactly(input, length, operationId))
            remaining -= length
        }
    }

    private fun verifyCompletedDigest(digest: MessageDigest, expected: ByteArray?) {
        if (expected?.size != ChunkCodec.DIGEST_SIZE || !digest.digest().contentEquals(expected)) {
            throw ResumeValidationException("Source digest does not match completed receipt")
        }
    }

    private fun registerOperation(
        pauseController: TransferPauseController? = null
    ): ActiveOperation {
        val operation = ActiveOperation(
            id = nextOperationId.incrementAndGet(),
            pauseController = pauseController
        )
        if (!activeOperation.compareAndSet(null, operation)) {
            throw IllegalStateException("Another client operation is already active")
        }
        return operation
    }

    private fun attachSocket(operationId: Long, socket: Socket) {
        while (true) {
            val current = activeOperation.get()
            if (current == null || current.id != operationId || current.cancelled) {
                runCatching { socket.close() }
                throw CancellationException("Client operation was cancelled")
            }
            val updated = current.copy(socket = socket)
            if (activeOperation.compareAndSet(current, updated)) {
                checkActive(operationId)
                return
            }
        }
    }

    private fun attachSource(operationId: Long, source: InputStream) {
        while (true) {
            val current = activeOperation.get()
            if (current == null || current.id != operationId || current.cancelled) {
                runCatching { source.close() }
                throw CancellationException("Client operation was cancelled")
            }
            val updated = current.copy(source = source)
            if (activeOperation.compareAndSet(current, updated)) {
                checkActive(operationId)
                return
            }
        }
    }

    private fun checkActive(operationId: Long) {
        val current = activeOperation.get()
        if (current == null || current.id != operationId || current.cancelled) {
            throw CancellationException("Client operation was cancelled")
        }
    }

    private fun clearOperation(operationId: Long) {
        while (true) {
            val current = activeOperation.get() ?: return
            if (current.id != operationId) return
            if (activeOperation.compareAndSet(current, null)) return
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

    private fun readExactly(input: InputStream, length: Int, operationId: Long): ByteArray {
        val data = ByteArray(length)
        var offset = 0
        while (offset < length) {
            checkActive(operationId)
            val read = input.read(data, offset, length - offset)
            checkActive(operationId)
            if (read < 0) throw EOFException("Source file ended early")
            if (read > 0) offset += read
        }
        return data
    }

    companion object {
        private const val CONNECT_TIMEOUT_MILLIS = 5_000
        private const val SOCKET_TIMEOUT_MILLIS = 15_000
        private const val QUERY_MIN_BYTES_PER_SECOND = 256L * 1024
        private const val MAX_ATTEMPTS = 3

        internal fun queryReadTimeoutMillis(fileSize: Long): Int {
            require(fileSize >= 0)
            val boundedSize = fileSize.coerceAtMost(TransferProtocol.MAX_FILE_SIZE)
            val wholeSeconds = boundedSize / QUERY_MIN_BYTES_PER_SECOND
            val scanSeconds = wholeSeconds +
                if (boundedSize % QUERY_MIN_BYTES_PER_SECOND == 0L) 0 else 1
            val maximum = Int.MAX_VALUE.toLong()
            val scanMillis = if (scanSeconds > maximum / 1_000L) {
                maximum
            } else {
                scanSeconds * 1_000L
            }
            return if (scanMillis > maximum - SOCKET_TIMEOUT_MILLIS) {
                Int.MAX_VALUE
            } else {
                (scanMillis + SOCKET_TIMEOUT_MILLIS).toInt()
            }
        }

        internal fun readyReadTimeoutMillis(confirmedBytes: Long): Int =
            queryReadTimeoutMillis(confirmedBytes)
    }

    private data class PreparedStream(
        val confirmedBytes: Long,
        val nextChunkIndex: Int,
        val wholeDigest: MessageDigest
    )

    private data class ActiveOperation(
        val id: Long,
        val socket: Socket? = null,
        val pauseController: TransferPauseController? = null,
        val source: InputStream? = null,
        val cancelled: Boolean = false
    )
}
