package com.example.transfer.transfer

import com.example.transfer.history.IncomingTransferHistory
import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.DigestChain
import com.example.transfer.protocol.ProtocolException
import com.example.transfer.protocol.ResumeProtocol
import com.example.transfer.protocol.TransferFrameCodec
import com.example.transfer.protocol.TransferFrameType
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.resume.IncomingResumeSession
import com.example.transfer.resume.ResumeCoordinator
import com.example.transfer.storage.IncomingFileStore
import com.example.transfer.storage.ReceivedFileHandle
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

fun interface ChunkVerifier {
    fun matches(data: ByteArray, expectedDigest: ByteArray): Boolean

    companion object {
        val SHA256 = ChunkVerifier { data, digest -> ChunkCodec.sha256(data).contentEquals(digest) }
    }
}

class FileTransferServer(
    private val port: Int = DEFAULT_PORT,
    private val store: IncomingFileStore? = null,
    private val chunkVerifier: ChunkVerifier = ChunkVerifier.SHA256,
    private val onTransferStart: () -> Boolean = { true },
    private val onTransferEnd: () -> Unit = {},
    private val pausedSessionLeaseMillis: Int = DEFAULT_PAUSED_SESSION_LEASE_MILLIS,
    private val monotonicMillis: () -> Long = { System.nanoTime() / 1_000_000 },
    private val history: IncomingTransferHistory = IncomingTransferHistory.None,
    private val resumeCoordinator: ResumeCoordinator? = null
) {
    init {
        require(pausedSessionLeaseMillis > 0) { "Paused session lease must be positive" }
    }

    @Volatile private var serverSocket: ServerSocket? = null
    private var job: Job? = null
    private val clients = Collections.newSetFromMap(ConcurrentHashMap<ReceiveSession, Boolean>())
    private val clientJobs = Collections.newSetFromMap(ConcurrentHashMap<Job, Boolean>())

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
                        clients += receiveSession
                        val child = launch(Dispatchers.IO) {
                            try {
                                if (!isActive) receiveSession.requestLocalStop()
                                runCatching { receive(receiveSession, onProgress, onComplete) }
                                    .onFailure { if (isActive) onError(it.message ?: "Receive failed") }
                            } finally {
                                clients -= receiveSession
                            }
                        }
                        clientJobs += child
                        child.invokeOnCompletion { clientJobs -= child }
                    }
                }
            } catch (error: Exception) {
                if (isActive) onError(error.message ?: "Receive service failed")
            } finally {
                serverSocket = null
            }
        }
    }

    fun stop() {
        val serverJob = job
        val children = clientJobs.toList()
        clients.forEach { it.requestLocalStop() }
        serverJob?.cancel()
        clients.forEach { runCatching { it.socket.close() } }
        children.forEach { it.cancel() }
        runCatching { serverSocket?.close() }
        serverSocket = null
        job = null
        runBlocking {
            children.forEach { runCatching { it.cancelAndJoin() } }
            serverJob?.let { runCatching { it.cancelAndJoin() } }
        }
    }

    private suspend fun receive(
        receiveSession: ReceiveSession,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit
    ) {
        if (resumeCoordinator != null) {
            receiveV4(receiveSession, resumeCoordinator, onProgress, onComplete)
        } else {
            receiveLegacy(receiveSession, onProgress, onComplete)
        }
    }

    private suspend fun receiveLegacy(
        receiveSession: ReceiveSession,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit
    ) {
        val socket = receiveSession.socket
        val legacyStore = requireNotNull(store)
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
                handle = legacyStore.create(header.fileName, header.mimeType)
                receiveChunks(socket, input, output, header.fileSize, handle, onProgress)
                val receivedUri = legacyStore.complete(handle)
                committed = true
                historyBestEffort { history.succeed(historyId, receivedUri) }
                output.writeByte(TransferProtocol.COMPLETE)
                output.flush()
                onComplete(handle.displayName)
            } catch (error: Exception) {
                if (!committed) {
                    val termination = receiveSession.classifyFailure(error)
                    withContext(NonCancellable) {
                        handle?.let { runCatching { legacyStore.abort(it) } }
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

    private suspend fun receiveV4(
        receiveSession: ReceiveSession,
        coordinator: ResumeCoordinator,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit
    ) {
        val socket = receiveSession.socket
        socket.use {
            socket.soTimeout = SOCKET_TIMEOUT_MILLIS
            val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
            val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            try {
                when (ConnectionProtocol.readPreamble(input)) {
                    ConnectionKind.RESUME_QUERY -> {
                        val offer = ResumeProtocol.readOffer(input)
                        ResumeProtocol.writeStatus(output, coordinator.queryIncoming(offer))
                        output.flush()
                    }
                    ConnectionKind.TRANSFER_START -> receiveV4Transfer(
                        receiveSession, coordinator, input, output, onProgress, onComplete
                    )
                    ConnectionKind.PAIR_REQUEST -> throw ProtocolException("Pairing is not installed")
                }
            } catch (error: Exception) {
                runCatching {
                    output.writeByte(TransferProtocol.FATAL)
                    output.flush()
                }
                throw error
            }
        }
    }

    private suspend fun receiveV4Transfer(
        receiveSession: ReceiveSession,
        coordinator: ResumeCoordinator,
        input: DataInputStream,
        output: DataOutputStream,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit
    ) {
        val offer = ResumeProtocol.readOffer(input)
        val mode = ResumeProtocol.readStartMode(input)
        val expectedStatus = ResumeProtocol.readStatus(input, offer)
        if (!onTransferStart()) throw ProtocolException("Another transfer is active")

        var session: IncomingResumeSession? = null
        var historyId: Long? = null
        var completed = false
        try {
            historyId = historyBestEffort {
                history.start(
                    fileName = offer.fileName,
                    fileSize = offer.fileSize,
                    mimeType = offer.mimeType,
                    peerAddress = receiveSession.socket.inetAddress.hostAddress
                        ?: receiveSession.socket.inetAddress.toString()
                )
            }
            session = coordinator.openIncoming(offer, mode, expectedStatus)
            session = receiveV4Chunks(
                receiveSession.socket, coordinator, session, offer.fileSize,
                input, output, onProgress, onCheckpoint = { session = it }
            )
            if (input.readUnsignedByte() != TransferProtocol.COMPLETE) {
                throw ProtocolException("Expected completion marker")
            }
            val senderDigest = ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully)
            val receiverDigest = session.prefix.wholeDigest.digest()
            if (!senderDigest.contentEquals(receiverDigest)) {
                throw ProtocolException("Whole-file digest mismatch")
            }
            val receivedUri = coordinator.completeIncoming(session)
            completed = true
            historyBestEffort { history.succeed(historyId, receivedUri) }
            output.writeByte(TransferProtocol.SUCCESS)
            output.flush()
            onComplete(session.handle.displayName)
        } catch (error: Exception) {
            if (!completed) {
                val termination = receiveSession.classifyFailure(error)
                withContext(NonCancellable) {
                    session?.let { runCatching { coordinator.releaseIncoming(it) } }
                    runCatching {
                        if (termination == ReceiveTermination.LOCAL_STOP) {
                            history.cancel(historyId, LOCAL_STOP_MESSAGE)
                        } else {
                            history.fail(historyId, error.message)
                        }
                    }
                }
            }
            throw error
        } finally {
            onTransferEnd()
        }
    }

    private suspend fun receiveV4Chunks(
        socket: Socket,
        coordinator: ResumeCoordinator,
        initialSession: IncomingResumeSession,
        total: Long,
        input: DataInputStream,
        output: DataOutputStream,
        onProgress: (String, Int) -> Unit,
        onCheckpoint: (IncomingResumeSession) -> Unit
    ): IncomingResumeSession {
        var session = initialSession
        var hasVerifiedChunkAck = session.checkpoint.nextChunkIndex > 0
        var pauseEntitled = false
        var remainingPauseLeaseMillis = pausedSessionLeaseMillis.toLong()
        while (session.checkpoint.confirmedBytes < total) {
            val type = TransferFrameCodec.readType(input)
            if (type == TransferFrameType.PAUSE) {
                if (!hasVerifiedChunkAck || !pauseEntitled) {
                    throw ProtocolException("Pause requires a new verified chunk ACK")
                }
                pauseEntitled = false
                remainingPauseLeaseMillis = acknowledgePause(
                    socket, input, output, remainingPauseLeaseMillis
                )
                continue
            }
            if (type != TransferFrameType.CHUNK) throw ProtocolException("Unexpected resume frame")

            val frame = readV4Chunk(input)
            val expectedIndex = session.checkpoint.nextChunkIndex
            val expectedLength = minOf(
                session.checkpoint.chunkSize.toLong(), total - session.checkpoint.confirmedBytes
            ).toInt()
            if (frame.index == expectedIndex - 1 && expectedIndex > 0) {
                val committedLength = if (session.checkpoint.confirmedBytes == total) {
                    ((total - 1) % session.checkpoint.chunkSize + 1).toInt()
                } else {
                    session.checkpoint.chunkSize
                }
                if (frame.data.size != committedLength ||
                    !frame.digest.contentEquals(session.checkpoint.lastChunkHash) ||
                    !chunkVerifier.matches(frame.data, frame.digest)
                ) throw ProtocolException("Committed chunk does not match checkpoint")
                output.writeByte(TransferProtocol.ACK)
                output.flush()
                continue
            }
            if (frame.index != expectedIndex || frame.data.size != expectedLength) {
                throw ProtocolException("Unexpected chunk")
            }
            if (!chunkVerifier.matches(frame.data, frame.digest)) {
                output.writeByte(TransferProtocol.NACK)
                output.flush()
                continue
            }

            session.handle.writeAt(session.checkpoint.confirmedBytes, frame.data, frame.data.size)
            session.prefix.wholeDigest.update(frame.data)
            val confirmed = session.checkpoint.confirmedBytes + frame.data.size
            val chain = DigestChain.next(
                session.checkpoint.chainDigest, frame.index, frame.data.size, frame.digest
            )
            session = coordinator.commitChunk(
                session = session,
                confirmedBytes = confirmed,
                nextChunkIndex = frame.index + 1,
                chainDigest = chain,
                lastChunkHash = frame.digest
            )
            onCheckpoint(session)
            output.writeByte(TransferProtocol.ACK)
            output.flush()
            hasVerifiedChunkAck = true
            pauseEntitled = true
            onProgress(session.handle.displayName, TransferProgress.percent(confirmed, total))
        }
        return session
    }

    private fun readV4Chunk(input: DataInputStream): com.example.transfer.protocol.ChunkFrame {
        val index = input.readInt()
        val length = input.readInt()
        if (index < 0 || length !in 1..TransferProtocol.CHUNK_SIZE) {
            throw ProtocolException("Invalid chunk")
        }
        val data = ByteArray(length).also(input::readFully)
        val digest = ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully)
        return com.example.transfer.protocol.ChunkFrame(index, data, digest)
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
