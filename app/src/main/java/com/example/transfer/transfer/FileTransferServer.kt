package com.example.transfer.transfer

import com.example.transfer.history.IncomingTransferHistory
import com.example.transfer.pairing.PairingProtocol
import com.example.transfer.pairing.PairingRequestHandler
import com.example.transfer.pairing.PairingResponse
import com.example.transfer.pairing.PairingStatus
import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.DigestChain
import com.example.transfer.protocol.ProtocolException
import com.example.transfer.protocol.ResumeProtocol
import com.example.transfer.protocol.TransferFrameCodec
import com.example.transfer.protocol.TransferFrameType
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.TransferStartResponse
import com.example.transfer.resume.IncomingResumeSession
import com.example.transfer.resume.ResumeCoordinator
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
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
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.Semaphore

fun interface ChunkVerifier {
    fun matches(data: ByteArray, expectedDigest: ByteArray): Boolean

    companion object {
        val SHA256 = ChunkVerifier { data, digest -> ChunkCodec.sha256(data).contentEquals(digest) }
    }
}

class FileTransferServer(
    private val port: Int = DEFAULT_PORT,
    private val resumeCoordinator: ResumeCoordinator,
    private val chunkVerifier: ChunkVerifier = ChunkVerifier.SHA256,
    private val onTransferStart: () -> Boolean = { true },
    private val onTransferEnd: () -> Unit = {},
    private val onTransferAttemptStart: ((Long) -> Boolean)? = null,
    private val onTransferAttemptEnd: ((Long) -> Unit)? = null,
    private val pausedSessionLeaseMillis: Int = DEFAULT_PAUSED_SESSION_LEASE_MILLIS,
    private val monotonicMillis: () -> Long = { System.nanoTime() / 1_000_000 },
    private val history: IncomingTransferHistory = IncomingTransferHistory.None,
    private val beforeClientRegister: () -> Unit = {},
    private val afterClientRegisterBeforeStart: () -> Unit = {},
    private val onClientStarted: () -> Unit = {},
    private val onServerStopping: () -> Unit = {},
    private val pairingRequestHandler: PairingRequestHandler = PairingRequestHandler {
        PairingResponse(PairingStatus.REJECTED)
    },
    maxActiveConnections: Int = MAX_ACTIVE_CONNECTIONS,
    maxConcurrentQueries: Int = MAX_CONCURRENT_QUERIES
) {
    init {
        require(pausedSessionLeaseMillis > 0) { "Paused session lease must be positive" }
        require(maxActiveConnections > 0 && maxConcurrentQueries > 0)
    }

    @Volatile private var serverSocket: ServerSocket? = null
    @Volatile private var boundPort: Int? = null
    private var job: Job? = null
    private val lifecycleLock = Any()
    private var stopping = true
    private val clients = linkedSetOf<ReceiveSession>()
    private val clientJobs = linkedSetOf<Job>()
    private val activeConnections = Semaphore(maxActiveConnections)
    private val concurrentQueries = Semaphore(maxConcurrentQueries)
    private val nextAttemptId = AtomicLong()

    fun start(
        scope: CoroutineScope,
        onStarted: (Int) -> Unit,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit,
        onError: (String) -> Unit
    ) = start(
        scope,
        onStarted,
        { _, name, progress -> onProgress(name, progress) },
        { _, name -> onComplete(name) },
        { _, message -> onError(message) }
    )

    fun start(
        scope: CoroutineScope,
        onStarted: (Int) -> Unit,
        onProgress: (Long, String, Int) -> Unit,
        onComplete: (Long, String) -> Unit,
        onError: (Long?, String) -> Unit
    ) {
        val serverJob = scope.launch(Dispatchers.IO, start = CoroutineStart.LAZY) {
            try {
                ServerSocket(port).use { server ->
                    val registered = synchronized(lifecycleLock) {
                        if (stopping) false else {
                            serverSocket = server
                            true
                        }
                    }
                    if (!registered) return@use
                    boundPort = server.localPort
                    onStarted(server.localPort)
                    while (isActive) {
                        val socket = server.accept()
                        if (!activeConnections.tryAcquire()) {
                            runCatching { socket.close() }
                            continue
                        }
                        try {
                            beforeClientRegister()
                        } catch (error: Exception) {
                            activeConnections.release()
                            runCatching { socket.close() }
                            throw error
                        }
                        val receiveSession = ReceiveSession(socket)
                        lateinit var child: Job
                        val released = AtomicBoolean(false)
                        val releaseRegistration = {
                            if (released.compareAndSet(false, true)) {
                                receiveSession.requestLocalStop()
                                runCatching { socket.close() }
                                synchronized(lifecycleLock) {
                                    clients -= receiveSession
                                    clientJobs -= child
                                }
                                activeConnections.release()
                            }
                            Unit
                        }
                        child = launch(Dispatchers.IO, start = CoroutineStart.LAZY) {
                            try {
                                onClientStarted()
                                if (!isActive) receiveSession.requestLocalStop()
                                runCatching { receive(receiveSession, onProgress, onComplete) }
                                    .onFailure {
                                        if (isActive) onError(
                                            receiveSession.attemptId,
                                            it.message ?: "Receive failed"
                                        )
                                    }
                            } finally {
                                if (receiveSession.attemptAdmitted) {
                                    receiveSession.attemptId?.let { attemptId ->
                                        onTransferAttemptEnd?.invoke(attemptId) ?: onTransferEnd()
                                    }
                                }
                                releaseRegistration()
                            }
                        }
                        child.invokeOnCompletion { releaseRegistration() }
                        val clientRegistered = synchronized(lifecycleLock) {
                            if (stopping) false else {
                                clients += receiveSession
                                clientJobs += child
                                true
                            }
                        }
                        if (clientRegistered) {
                            try {
                                afterClientRegisterBeforeStart()
                                if (!child.start()) releaseRegistration()
                            } catch (error: Exception) {
                                child.cancel()
                                releaseRegistration()
                                throw error
                            }
                        } else {
                            child.cancel()
                            releaseRegistration()
                        }
                    }
                }
            } catch (error: Exception) {
                if (isActive) onError(null, error.message ?: "Receive service failed")
            } finally {
                synchronized(lifecycleLock) {
                    serverSocket = null
                    boundPort = null
                }
            }
        }
        val shouldStart = synchronized(lifecycleLock) {
            if (job != null) false else {
                stopping = false
                job = serverJob
                true
            }
        }
        if (shouldStart) serverJob.start() else serverJob.cancel()
    }

    fun stop() {
        val snapshot = synchronized(lifecycleLock) {
            stopping = true
            runCatching { serverSocket?.close() }
            StopSnapshot(job, clients.toList(), clientJobs.toList()).also {
                job = null
                serverSocket = null
                boundPort = null
            }
        }
        onServerStopping()
        val serverJob = snapshot.serverJob
        val children = snapshot.children
        snapshot.sessions.forEach { it.requestLocalStop() }
        serverJob?.cancel()
        snapshot.sessions.forEach { runCatching { it.socket.close() } }
        children.forEach { it.cancel() }
        runBlocking {
            children.forEach { runCatching { it.cancelAndJoin() } }
            serverJob?.let { runCatching { it.cancelAndJoin() } }
        }
    }

    internal fun registeredClientCount(): Int = synchronized(lifecycleLock) {
        check(clients.size == clientJobs.size) { "Client registry is inconsistent" }
        clients.size
    }

    private data class StopSnapshot(
        val serverJob: Job?,
        val sessions: List<ReceiveSession>,
        val children: List<Job>
    )

    private suspend fun receive(
        receiveSession: ReceiveSession,
        onProgress: (Long, String, Int) -> Unit,
        onComplete: (Long, String) -> Unit
    ) {
        receiveV4(receiveSession, resumeCoordinator, onProgress, onComplete)
    }

    private suspend fun receiveV4(
        receiveSession: ReceiveSession,
        coordinator: ResumeCoordinator,
        onProgress: (Long, String, Int) -> Unit,
        onComplete: (Long, String) -> Unit
    ) {
        val socket = receiveSession.socket
        socket.use {
            socket.soTimeout = SOCKET_TIMEOUT_MILLIS
            val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
            val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            try {
                when (ConnectionProtocol.readPreamble(input)) {
                    ConnectionKind.RESUME_QUERY -> {
                        if (!concurrentQueries.tryAcquire()) {
                            throw ProtocolException("Too many concurrent resume queries")
                        }
                        try {
                            val offer = ResumeProtocol.readOffer(input)
                            ResumeProtocol.writeStatus(output, coordinator.queryIncoming(offer))
                            output.flush()
                        } finally {
                            concurrentQueries.release()
                        }
                    }
                    ConnectionKind.TRANSFER_START -> receiveV4Transfer(
                        receiveSession, coordinator, input, output, onProgress, onComplete
                    )
                    ConnectionKind.PAIR_REQUEST -> receivePairing(input, output)
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

    fun listeningPort(): Int? = boundPort

    private fun receivePairing(input: DataInputStream, output: DataOutputStream) {
        val response = try {
            pairingRequestHandler.handle(PairingProtocol.readRequest(input))
        } catch (_: Exception) {
            PairingResponse(PairingStatus.PROTOCOL_ERROR)
        }
        PairingProtocol.writeResponse(output, response)
        output.flush()
    }

    private suspend fun receiveV4Transfer(
        receiveSession: ReceiveSession,
        coordinator: ResumeCoordinator,
        input: DataInputStream,
        output: DataOutputStream,
        onProgress: (Long, String, Int) -> Unit,
        onComplete: (Long, String) -> Unit
    ) {
        val offer = ResumeProtocol.readOffer(input)
        val mode = ResumeProtocol.readStartMode(input)
        val expectedStatus = ResumeProtocol.readStatus(input, offer)
        coordinator.queryCompleted(offer)?.let { completed ->
            if (completed.state == ResumeState.COMPLETED) {
                ResumeProtocol.writeStartResponse(
                    output, TransferStartResponse.COMPLETED, completed.finalDigest
                )
                output.flush()
                return
            }
            ResumeProtocol.writeStartResponse(output, TransferStartResponse.FATAL)
            output.flush()
            return
        }
        val attemptId = nextAttemptId.incrementAndGet()
        receiveSession.attemptId = attemptId
        val started = onTransferAttemptStart?.invoke(attemptId) ?: onTransferStart()
        if (!started) throw ProtocolException("Another transfer is active")
        receiveSession.attemptAdmitted = true

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
            session = try {
                coordinator.openIncoming(offer, mode, expectedStatus)
            } catch (error: Exception) {
                val completionStatus = coordinator.queryCompleted(offer)
                if (completionStatus?.state == ResumeState.COMPLETED) {
                    ResumeProtocol.writeStartResponse(
                        output, TransferStartResponse.COMPLETED, completionStatus.finalDigest
                    )
                    output.flush()
                    completed = true
                    return
                }
                throw error
            }
            ResumeProtocol.writeStartResponse(output, TransferStartResponse.READY)
            output.flush()
            val received = receiveV4Frames(
                receiveSession.socket, coordinator, session, offer.fileSize,
                input, output,
                { name, progress -> onProgress(attemptId, name, progress) },
                onCheckpoint = { session = it }
            )
            session = received.session
            val senderDigest = received.wholeDigest
            val receiverDigest = session.prefix.wholeDigest.digest()
            if (!senderDigest.contentEquals(receiverDigest)) {
                throw ProtocolException("Whole-file digest mismatch")
            }
            val receivedUri = coordinator.completeIncoming(session, senderDigest)
            completed = true
            historyBestEffort { history.succeed(historyId, receivedUri) }
            output.writeByte(TransferProtocol.SUCCESS)
            output.flush()
            onComplete(attemptId, session.handle.displayName)
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
        }
    }

    private suspend fun receiveV4Frames(
        socket: Socket,
        coordinator: ResumeCoordinator,
        initialSession: IncomingResumeSession,
        total: Long,
        input: DataInputStream,
        output: DataOutputStream,
        onProgress: (String, Int) -> Unit,
        onCheckpoint: (IncomingResumeSession) -> Unit
    ): CompletedFrames {
        var session = initialSession
        var hasVerifiedChunkAck = session.checkpoint.nextChunkIndex > 0
        var pauseEntitled = false
        var consecutiveRejected = 0
        var remainingPauseLeaseMillis = pausedSessionLeaseMillis.toLong()
        while (true) {
            val typeCode = input.readUnsignedByte()
            if (typeCode == TransferProtocol.COMPLETE) {
                if (session.checkpoint.confirmedBytes != total) {
                    throw ProtocolException("Transfer completed before all bytes were committed")
                }
                return CompletedFrames(
                    session,
                    ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully)
                )
            }
            val type = TransferFrameType.entries.firstOrNull { it.code == typeCode }
                ?: throw ProtocolException("Unknown transfer frame")
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
                consecutiveRejected++
                if (consecutiveRejected > MAX_REJECTED_CHUNK_ATTEMPTS) {
                    throw ProtocolException("Too many duplicate chunk attempts")
                }
                output.writeByte(TransferProtocol.ACK)
                output.flush()
                continue
            }
            if (frame.index != expectedIndex || frame.data.size != expectedLength) {
                throw ProtocolException("Unexpected chunk")
            }
            if (!chunkVerifier.matches(frame.data, frame.digest)) {
                consecutiveRejected++
                if (consecutiveRejected > MAX_REJECTED_CHUNK_ATTEMPTS) {
                    throw ProtocolException("Too many invalid chunk attempts")
                }
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
            consecutiveRejected = 0
            onCheckpoint(session)
            output.writeByte(TransferProtocol.ACK)
            output.flush()
            hasVerifiedChunkAck = true
            pauseEntitled = true
            onProgress(session.handle.displayName, TransferProgress.percent(confirmed, total))
        }
    }

    private data class CompletedFrames(
        val session: IncomingResumeSession,
        val wholeDigest: ByteArray
    )

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
        @Volatile var attemptId: Long? = null
        @Volatile var attemptAdmitted: Boolean = false

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
        private const val LOCAL_STOP_MESSAGE = "Receive service stopped"
        internal const val MAX_ACTIVE_CONNECTIONS = 16
        internal const val MAX_CONCURRENT_QUERIES = 2
        internal const val MAX_REJECTED_CHUNK_ATTEMPTS = 3
    }
}
