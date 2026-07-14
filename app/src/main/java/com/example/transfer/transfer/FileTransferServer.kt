package com.example.transfer.transfer

import com.example.transfer.history.IncomingTransferHistory
import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.ProtocolException
import com.example.transfer.protocol.TransferFrameCodec
import com.example.transfer.protocol.TransferFrameType
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.storage.IncomingFileStore
import com.example.transfer.storage.ReceivedFileHandle
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicReference

fun interface ChunkVerifier {
    fun matches(data: ByteArray, expectedDigest: ByteArray): Boolean

    companion object {
        val SHA256 = ChunkVerifier { data, digest -> ChunkCodec.sha256(data).contentEquals(digest) }
    }
}

class FileTransferServer(
    private val port: Int = DEFAULT_PORT,
    private val store: IncomingFileStore,
    private val chunkVerifier: ChunkVerifier = ChunkVerifier.SHA256,
    private val onTransferStart: () -> Boolean = { true },
    private val onTransferEnd: () -> Unit = {},
    private val pausedSessionLeaseMillis: Int = DEFAULT_PAUSED_SESSION_LEASE_MILLIS,
    private val monotonicMillis: () -> Long = { System.nanoTime() / 1_000_000 },
    private val history: IncomingTransferHistory = IncomingTransferHistory.None
) {
    init {
        require(pausedSessionLeaseMillis > 0) { "Paused session lease must be positive" }
    }

    @Volatile private var serverSocket: ServerSocket? = null
    @Volatile private var activeReceive: ReceiveSession? = null
    private var job: Job? = null

    fun start(
        scope: CoroutineScope,
        onStarted: (Int) -> Unit,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit,
        onError: (String) -> Unit
    ) {
        if (job?.isActive == true) return
        job = scope.launch(Dispatchers.IO) {
            try {
                ServerSocket(port).use { server ->
                    serverSocket = server
                    onStarted(server.localPort)
                    while (isActive) {
                        val socket = server.accept()
                        val receiveSession = ReceiveSession(socket)
                        activeReceive = receiveSession
                        try {
                            if (!isActive) {
                                receiveSession.requestLocalStop()
                                runCatching { socket.close() }
                                break
                            }
                            runCatching { receive(receiveSession, onProgress, onComplete) }
                                .onFailure { if (isActive) onError(it.message ?: "Receive failed") }
                        } finally {
                            if (activeReceive === receiveSession) activeReceive = null
                        }
                    }
                }
            } catch (error: Exception) {
                if (isActive) onError(error.message ?: "Receive service failed")
            } finally {
                serverSocket = null
                activeReceive = null
            }
        }
    }

    fun stop() {
        val receiveSession = activeReceive
        receiveSession?.requestLocalStop()
        job?.cancel()
        runCatching { receiveSession?.socket?.close() }
        runCatching { serverSocket?.close() }
        activeReceive = null
        serverSocket = null
        job = null
    }

    private suspend fun receive(
        receiveSession: ReceiveSession,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit
    ) {
        val socket = receiveSession.socket
        socket.use {
            socket.soTimeout = SOCKET_TIMEOUT_MILLIS
            val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
            val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            var handle: ReceivedFileHandle? = null
            var transferStarted = false
            var historyId: Long? = null
            var committed = false
            try {
                val header = TransferProtocol.readHeader(input)
                if (!onTransferStart()) error("Another transfer is active")
                transferStarted = true
                historyId = historyBestEffort {
                    history.start(
                        fileName = header.fileName,
                        fileSize = header.fileSize,
                        mimeType = header.mimeType,
                        peerAddress = socket.inetAddress.hostAddress ?: socket.inetAddress.toString()
                    )
                }
                handle = store.create(header.fileName, header.mimeType)
                receiveChunks(socket, input, output, header.fileSize, handle, onProgress)
                val receivedUri = store.complete(handle)
                committed = true
                historyBestEffort { history.succeed(historyId, receivedUri) }
                output.writeByte(TransferProtocol.COMPLETE)
                output.flush()
                onComplete(handle.displayName)
            } catch (error: Exception) {
                if (!committed) {
                    val termination = receiveSession.classifyFailure(error)
                    withContext(NonCancellable) {
                        handle?.let { runCatching { store.abort(it) } }
                        runCatching {
                            if (termination == ReceiveTermination.LOCAL_STOP) {
                                history.cancel(historyId, LOCAL_STOP_MESSAGE)
                            } else {
                                history.fail(historyId, error.message)
                            }
                        }
                    }
                }
                runCatching { output.writeByte(TransferProtocol.FATAL); output.flush() }
                throw error
            } finally {
                if (transferStarted) onTransferEnd()
            }
        }
    }

    private suspend fun <T> historyBestEffort(block: suspend () -> T): T? = try {
        block()
    } catch (error: kotlinx.coroutines.CancellationException) {
        throw error
    } catch (_: Exception) {
        null
    }

    private class ReceiveSession(val socket: Socket) {
        private val termination = AtomicReference(ReceiveTermination.ACTIVE)

        fun requestLocalStop() {
            termination.compareAndSet(ReceiveTermination.ACTIVE, ReceiveTermination.LOCAL_STOP)
        }

        fun classifyFailure(error: Exception): ReceiveTermination {
            val failure = if (error is kotlinx.coroutines.CancellationException) {
                ReceiveTermination.LOCAL_STOP
            } else {
                ReceiveTermination.FAILURE
            }
            termination.compareAndSet(ReceiveTermination.ACTIVE, failure)
            return termination.get()
        }
    }

    private enum class ReceiveTermination {
        ACTIVE,
        LOCAL_STOP,
        FAILURE
    }

    private fun receiveChunks(
        socket: Socket,
        input: DataInputStream,
        output: DataOutputStream,
        total: Long,
        handle: ReceivedFileHandle,
        onProgress: (String, Int) -> Unit
    ) {
        var received = 0L
        var index = 0
        var hasVerifiedChunkAck = false
        var pauseEntitled = false
        var remainingPauseLeaseMillis = pausedSessionLeaseMillis.toLong()
        while (received < total) {
            val expectedLength = minOf(TransferProtocol.CHUNK_SIZE.toLong(), total - received).toInt()
            var failures = 0
            while (true) {
                val type = TransferFrameCodec.readType(input)
                if (type == TransferFrameType.PAUSE) {
                    if (!hasVerifiedChunkAck) {
                        throw ProtocolException("Pause before first verified chunk ACK")
                    }
                    if (!pauseEntitled) {
                        throw ProtocolException("Pause requires a new verified chunk ACK")
                    }
                    if (failures > 0) throw ProtocolException("Control frame during chunk retry")
                    pauseEntitled = false
                    remainingPauseLeaseMillis = acknowledgePause(
                        socket,
                        input,
                        output,
                        remainingPauseLeaseMillis
                    )
                    continue
                }
                if (type == TransferFrameType.RESUME) {
                    throw ProtocolException("Unexpected resume frame")
                }
                val frame = ChunkCodec.read(input, index, expectedLength)
                if (chunkVerifier.matches(frame.data, frame.digest)) {
                    handle.output.write(frame.data)
                    received += frame.data.size
                    output.writeByte(TransferProtocol.ACK)
                    output.flush()
                    hasVerifiedChunkAck = true
                    pauseEntitled = true
                    onProgress(handle.displayName, TransferProgress.percent(received, total))
                    break
                }
                failures++
                output.writeByte(TransferProtocol.NACK)
                output.flush()
                if (failures >= MAX_ATTEMPTS) error("Chunk $index failed verification")
            }
            index++
        }
    }

    private fun acknowledgePause(
        socket: Socket,
        input: DataInputStream,
        output: DataOutputStream,
        remainingLeaseMillis: Long
    ): Long {
        if (remainingLeaseMillis <= 0) {
            throw ProtocolException("Paused session lease expired")
        }
        val originalTimeout = socket.soTimeout
        val pauseStartedAt = monotonicMillis()
        socket.soTimeout = remainingLeaseMillis.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        try {
            output.writeByte(TransferProtocol.CONTROL_ACK)
            output.flush()
            if (TransferFrameCodec.readType(input) != TransferFrameType.RESUME) {
                throw ProtocolException("Expected resume frame")
            }
            val elapsed = (monotonicMillis() - pauseStartedAt).coerceAtLeast(0)
            if (elapsed >= remainingLeaseMillis) {
                throw ProtocolException("Paused session lease expired")
            }
            output.writeByte(TransferProtocol.CONTROL_ACK)
            output.flush()
            return remainingLeaseMillis - elapsed
        } catch (_: SocketTimeoutException) {
            throw ProtocolException("Paused session lease expired")
        } finally {
            socket.soTimeout = originalTimeout
        }
    }

    companion object {
        const val DEFAULT_PORT = 42043
        private const val DEFAULT_PAUSED_SESSION_LEASE_MILLIS = 30 * 60 * 1000
        private const val SOCKET_TIMEOUT_MILLIS = 15_000
        private const val MAX_ATTEMPTS = 3
        private const val LOCAL_STOP_MESSAGE = "Receive service stopped"
    }
}
