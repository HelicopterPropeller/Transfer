package com.example.transfer.transfer

import com.example.transfer.history.IncomingTransferHistory
import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.ResumeProtocol
import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferFrameCodec
import com.example.transfer.protocol.TransferFrameType
import com.example.transfer.protocol.TransferOffer
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.resume.IncomingCheckpoint
import com.example.transfer.resume.CompletedReceipt
import com.example.transfer.resume.IncomingOperationState
import com.example.transfer.resume.IncomingStagingJournal
import com.example.transfer.resume.OutgoingResumeLink
import com.example.transfer.resume.PreparedTransfer
import com.example.transfer.resume.ResumeCoordinator
import com.example.transfer.resume.ResumeStore
import com.example.transfer.storage.ResumableFileHandle
import com.example.transfer.storage.ResumableIncomingFileStore
import com.example.transfer.storage.StoredFileLocation
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.Socket
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class FileTransferServerV4Test {
    @Test
    fun `pause lease is cumulative across acknowledgements for multiple chunks`() = runBlocking {
        val monotonic = AtomicLong(0)
        val harness = V4Harness(
            pausedSessionLeaseMillis = 100,
            monotonicMillis = monotonic::get
        )
        val chunk = ByteArray(TransferProtocol.CHUNK_SIZE) { 3 }
        val offer = harness.offer(chunk.size.toLong() * 2 + 1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)

                sendChunk(output, input, 0, chunk)
                pauseAndResume(output, input) { monotonic.set(60) }
                sendChunk(output, input, 1, chunk)
                TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                output.flush()
                assertEquals(TransferProtocol.CONTROL_ACK, input.readUnsignedByte())
                monotonic.set(101)
                TransferFrameCodec.writeType(output, TransferFrameType.RESUME)
                output.flush()
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
            withTimeout(2_000) { harness.history.failed.await() }
            assertEquals(chunk.size.toLong() * 2, harness.store.findIncoming(offer.transferId)?.confirmedBytes)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `one acknowledged chunk grants at most one pause`() = runBlocking {
        val harness = V4Harness()
        val chunk = ByteArray(TransferProtocol.CHUNK_SIZE) { 4 }
        val offer = harness.offer(chunk.size.toLong() + 1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                sendChunk(output, input, 0, chunk)
                pauseAndResume(output, input) {}

                TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                output.flush()
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
            withTimeout(2_000) { harness.history.failed.await() }
            assertEquals(chunk.size.toLong(), harness.store.findIncoming(offer.transferId)?.confirmedBytes)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `incoming attempt callbacks carry one stable id through terminal end`() = runBlocking {
        val harness = V4Harness(recordAttempts = true)
        val bytes = byteArrayOf(7)
        try {
            val result = FileTransferClient().sendPrepared(
                InetAddress.getLoopbackAddress(), harness.port(), harness.prepared(bytes),
                TransferStartMode.NEW, ResumeStatus(ResumeState.NONE),
                TransferPauseController(), {}, {}
            )
            assertTrue(result.isSuccess)
            withTimeout(2_000) {
                while (harness.attemptEvents.none { it.first == "end" }) delay(10)
            }
            val events = harness.attemptEvents.toList()
            assertTrue(events.map { it.first }.containsAll(listOf("start", "progress", "complete", "end")))
            assertEquals(1, events.map { it.second }.toSet().size)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `incoming failure callback precedes matching attempt end`() = runBlocking {
        val harness = V4Harness(recordAttempts = true)
        val offer = harness.offer(1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                output.flush()
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
            withTimeout(2_000) {
                while (harness.attemptEvents.none { it.first == "end" }) delay(10)
            }
            val events = harness.attemptEvents.toList()
            val errorIndex = events.indexOfFirst { it.first == "error" }
            val endIndex = events.indexOfFirst { it.first == "end" }
            assertTrue(errorIndex >= 0)
            assertTrue(errorIndex < endIndex)
            assertEquals(events[errorIndex].second, events[endIndex].second)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `server defaults bound active sockets and concurrent queries`() {
        assertEquals(16, FileTransferServer.MAX_ACTIVE_CONNECTIONS)
        assertEquals(2, FileTransferServer.MAX_CONCURRENT_QUERIES)
    }

    @Test
    fun `active socket admission closes connection beyond configured limit`() = runBlocking {
        val harness = V4Harness(maxActiveConnections = 1)
        val first = Socket(InetAddress.getLoopbackAddress(), harness.port())
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { second ->
                second.soTimeout = 2_000
                assertEquals(-1, second.getInputStream().read())
            }
        } finally {
            first.close()
            harness.close()
        }
    }

    @Test
    fun `query admission rejects new work when configured permits are occupied`() = runBlocking {
        val harness = V4Harness(maxConcurrentQueries = 1)
        val chunk = ByteArray(TransferProtocol.CHUNK_SIZE) { 1 }
        val offer = harness.offer(chunk.size.toLong() + 1)
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, ChunkCodec.create(0, chunk))
                output.flush()
                assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
            }
            withTimeout(2_000) { while (!harness.store.isIdle(offer.transferId)) delay(10) }
            harness.files.pauseFirstQueryRead(entered, release)
            val first = async(Dispatchers.IO) { harness.query(offer) }
            assertTrue(entered.await(2, TimeUnit.SECONDS))
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                ConnectionProtocol.writePreamble(output, ConnectionKind.RESUME_QUERY)
                ResumeProtocol.writeOffer(output, offer)
                output.flush()
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
            release.countDown()
            assertEquals(ResumeState.AVAILABLE, first.await().state)
        } finally {
            release.countDown()
            harness.close()
        }
    }
    @Test
    fun `pause lease timeout retains committed checkpoint`() = runBlocking {
        val harness = V4Harness(pausedSessionLeaseMillis = 100)
        val first = ByteArray(TransferProtocol.CHUNK_SIZE) { 2 }
        val offer = harness.offer(first.size.toLong() + 1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, ChunkCodec.create(0, first))
                output.flush()
                assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
                TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                output.flush()
                assertEquals(TransferProtocol.CONTROL_ACK, input.readUnsignedByte())
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
            withTimeout(2_000) { harness.history.failed.await() }
            assertEquals(first.size.toLong(), harness.store.findIncoming(offer.transferId)?.confirmedBytes)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `history failure callback never replaces original transfer error`() = runBlocking {
        val history = SocketHistory(failFailure = IOException("history unavailable"))
        val harness = V4Harness(
            verifier = ChunkVerifier { _, _ -> error("original transfer failure") },
            configuredHistory = history
        )
        val prepared = harness.prepared(byteArrayOf(1))
        try {
            val result = FileTransferClient().sendPrepared(
                InetAddress.getLoopbackAddress(), harness.port(), prepared,
                TransferStartMode.NEW, ResumeStatus(ResumeState.NONE),
                TransferPauseController(), {}, {}
            )
            assertTrue(result.isFailure)
            withTimeout(2_000) { history.failed.await() }
            withTimeout(2_000) {
                while (harness.errors.isEmpty()) delay(10)
            }
            assertTrue(harness.errors.single().contains("original transfer failure"))
        } finally {
            harness.close()
        }
    }

    @Test
    fun `success response write failure after publish does not undo success`() = runBlocking {
        val harness = V4Harness()
        val publishEntered = CountDownLatch(1)
        val releasePublish = CountDownLatch(1)
        harness.files.pausePublish(publishEntered, releasePublish)
        val bytes = byteArrayOf(42)
        val offer = harness.offer(bytes.size.toLong())
        var socket: Socket? = null
        try {
            socket = Socket(InetAddress.getLoopbackAddress(), harness.port())
            val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
            val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
            assertReady(input)
            TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
            ChunkCodec.write(output, ChunkCodec.create(0, bytes))
            output.flush()
            assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
            output.writeByte(TransferProtocol.COMPLETE)
            output.write(MessageDigest.getInstance("SHA-256").digest(bytes))
            output.flush()
            assertTrue(publishEntered.await(2, TimeUnit.SECONDS))
            socket.setSoLinger(true, 0)
            socket.close()
            socket = null
            releasePublish.countDown()

            withTimeout(2_000) { harness.history.succeeded.await() }
            assertTrue(harness.files.published)
            assertEquals(null, harness.store.findIncoming(offer.transferId))
            val completed = harness.query(offer)
            assertEquals(ResumeState.COMPLETED, completed.state)
            val replay = FileTransferClient().sendPrepared(
                InetAddress.getLoopbackAddress(), harness.port(),
                PreparedTransfer(
                    offer,
                    SendFileSource(
                        offer.fileName, offer.mimeType, offer.fileSize,
                        sourceUri = "content://replay", openStream = { ByteArrayInputStream(bytes) }
                    ),
                    ResumeStatus(ResumeState.NONE)
                ),
                TransferStartMode.NEW,
                ResumeStatus(ResumeState.NONE),
                TransferPauseController(), {}, {}
            )
            assertTrue(replay.isSuccess)
            assertEquals(1, harness.files.createCount)
            assertEquals(1, harness.files.writeCount)
            assertEquals(1, harness.files.publishCount)
        } finally {
            releasePublish.countDown()
            socket?.close()
            harness.close()
        }
    }

    @Test
    fun `nack retries identical v4 chunk and then succeeds`() = runBlocking {
        val checks = AtomicInteger()
        val harness = V4Harness(
            verifier = ChunkVerifier { data, digest ->
                checks.incrementAndGet() > 1 && ChunkCodec.sha256(data).contentEquals(digest)
            }
        )
        val bytes = byteArrayOf(1, 2, 3, 4)
        try {
            val result = FileTransferClient().sendPrepared(
                InetAddress.getLoopbackAddress(), harness.port(), harness.prepared(bytes),
                TransferStartMode.NEW, ResumeStatus(ResumeState.NONE),
                TransferPauseController(), {}, {}
            )
            assertTrue(result.exceptionOrNull()?.message, result.isSuccess)
            assertEquals(2, checks.get())
            assertEquals(1, harness.files.writeCount)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `three nacks fail attempt but retain zero-byte checkpoint`() = runBlocking {
        val harness = V4Harness(verifier = ChunkVerifier { _, _ -> false })
        val bytes = byteArrayOf(1, 2, 3)
        val prepared = harness.prepared(bytes)
        try {
            val result = FileTransferClient().sendPrepared(
                InetAddress.getLoopbackAddress(), harness.port(), prepared,
                TransferStartMode.NEW, ResumeStatus(ResumeState.NONE),
                TransferPauseController(), {}, {}
            )
            assertTrue(result.isFailure)
            withTimeout(2_000) { harness.history.failed.await() }
            val checkpoint = harness.store.findIncoming(prepared.offer.transferId)
            assertEquals(0L, checkpoint?.confirmedBytes)
            assertFalse(harness.files.published)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `pause before first durable ack is fatal`() = runBlocking {
        val harness = V4Harness()
        val offer = harness.offer(1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                output.flush()
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
            withTimeout(2_000) { harness.history.failed.await() }
            assertTrue(harness.store.findIncoming(offer.transferId) != null)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `pause after durable v4 ack resumes remaining chunks`() = runBlocking {
        val harness = V4Harness()
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE + 1) { (it % 251).toByte() }
        val controller = TransferPauseController()
        val paused = CompletableDeferred<Unit>()
        try {
            val sending = async(Dispatchers.IO) {
                FileTransferClient().sendPrepared(
                    InetAddress.getLoopbackAddress(), harness.port(), harness.prepared(bytes),
                    TransferStartMode.NEW, ResumeStatus(ResumeState.NONE), controller,
                    { if (it == TransferPauseState.PAUSED) paused.complete(Unit) },
                    { if (it.confirmedBytes == TransferProtocol.CHUNK_SIZE.toLong()) controller.requestPause() }
                )
            }
            withTimeout(2_000) { paused.await() }
            assertFalse(sending.isCompleted)
            controller.requestResume()
            assertTrue(withTimeout(3_000) { sending.await() }.isSuccess)
            assertArrayEquals(bytes, harness.files.bytes(harness.lastOfferId))
        } finally {
            harness.close()
        }
    }

    @Test
    fun `stop closes socket accepted during registration window without starting child`() = runBlocking {
        val accepted = CountDownLatch(1)
        val releaseRegistration = CountDownLatch(1)
        val stopping = CountDownLatch(1)
        val childStarts = AtomicInteger()
        val job = SupervisorJob()
        val scope = CoroutineScope(job + Dispatchers.IO)
        val ready = CompletableDeferred<Int>()
        val server = FileTransferServer(
            port = 0,
            resumeCoordinator = ResumeCoordinator(SocketResumeStore(), SocketFiles()),
            beforeClientRegister = {
                accepted.countDown()
                check(releaseRegistration.await(2, TimeUnit.SECONDS))
            },
            onClientStarted = { childStarts.incrementAndGet() },
            onServerStopping = { stopping.countDown() }
        )
        server.start(scope, { ready.complete(it) }, { _, _ -> }, {}, {})
        val socket = Socket(InetAddress.getLoopbackAddress(), withTimeout(2_000) { ready.await() })
        try {
            assertTrue(accepted.await(2, TimeUnit.SECONDS))
            val stopped = async(Dispatchers.IO) { server.stop() }
            assertTrue(stopping.await(2, TimeUnit.SECONDS))
            releaseRegistration.countDown()
            withTimeout(2_000) { stopped.await() }
            socket.soTimeout = 2_000
            assertEquals(-1, socket.getInputStream().read())
            assertEquals(0, childStarts.get())
        } finally {
            releaseRegistration.countDown()
            socket.close()
            server.stop()
            job.cancelAndJoin()
        }
    }

    @Test
    fun `stop after lazy child registration releases permit and registry before restart`() = runBlocking {
        val registered = CountDownLatch(1)
        val releaseStart = CountDownLatch(1)
        val hookCalls = AtomicInteger()
        val childStarts = AtomicInteger()
        val job = SupervisorJob()
        val scope = CoroutineScope(job + Dispatchers.IO)
        val server = FileTransferServer(
            port = 0,
            resumeCoordinator = ResumeCoordinator(SocketResumeStore(), SocketFiles()),
            onClientStarted = { childStarts.incrementAndGet() },
            afterClientRegisterBeforeStart = {
                if (hookCalls.incrementAndGet() == 1) {
                    registered.countDown()
                    check(releaseStart.await(2, TimeUnit.SECONDS))
                }
            }
        )
        val firstReady = CompletableDeferred<Int>()
        server.start(scope, { firstReady.complete(it) }, { _, _ -> }, {}, {})
        val first = Socket(InetAddress.getLoopbackAddress(), withTimeout(2_000) { firstReady.await() })
        try {
            assertTrue(registered.await(2, TimeUnit.SECONDS))
            val stopping = async(Dispatchers.IO) { server.stop() }
            releaseStart.countDown()
            withTimeout(2_000) { stopping.await() }
            assertEquals(0, server.registeredClientCount())

            repeat(3) { cycle ->
                val ready = CompletableDeferred<Int>()
                server.start(scope, { ready.complete(it) }, { _, _ -> }, {}, {})
                val port = withTimeout(2_000) { ready.await() }
                val sockets = (1..FileTransferServer.MAX_ACTIVE_CONNECTIONS).map {
                    Socket(InetAddress.getLoopbackAddress(), port)
                }
                try {
                    withTimeout(2_000) {
                        val expectedStarts = (cycle + 1) * FileTransferServer.MAX_ACTIVE_CONNECTIONS
                        while (childStarts.get() < expectedStarts) delay(10)
                    }
                    assertEquals(FileTransferServer.MAX_ACTIVE_CONNECTIONS, server.registeredClientCount())
                } finally {
                    server.stop()
                    sockets.forEach { runCatching { it.close() } }
                }
                assertEquals(0, server.registeredClientCount())
            }
        } finally {
            releaseStart.countDown()
            first.close()
            server.stop()
            job.cancelAndJoin()
        }
    }

    @Test
    fun `stop cancels resume query during cooperative large prefix scan`() = runBlocking {
        val harness = V4Harness()
        val chunk = ByteArray(TransferProtocol.CHUNK_SIZE) { 5 }
        val offer = harness.offer(chunk.size.toLong() * 2 + 1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                repeat(2) { index ->
                    TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                    ChunkCodec.write(output, ChunkCodec.create(index, chunk))
                    output.flush()
                    assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
                }
            }
            withTimeout(2_000) {
                while (!harness.store.isIdle(offer.transferId)) delay(10)
            }
            val enteredRead = CountDownLatch(1)
            val releaseRead = CountDownLatch(1)
            harness.files.pauseFirstQueryRead(enteredRead, releaseRead)
            val queryScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
            val query = queryScope.async { harness.query(offer) }
            assertTrue(enteredRead.await(2, TimeUnit.SECONDS))
            val stopped = async(Dispatchers.IO) { harness.stop() }
            assertTrue(harness.awaitStopping())
            releaseRead.countDown()

            withTimeout(2_000) { stopped.await() }
            assertTrue(runCatching { withTimeout(2_000) { query.await() } }.isFailure)
            queryScope.cancel()
        } finally {
            harness.close()
        }
    }

    @Test
    fun `sendPrepared completes through v4 server`() = runBlocking {
        val harness = V4Harness()
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE + 17) { (it % 251).toByte() }
        val offer = harness.offer(bytes.size.toLong())
        val prepared = PreparedTransfer(
            offer,
            SendFileSource(
                "a.bin", "application/octet-stream", bytes.size.toLong(),
                sourceUri = "content://source/a", openStream = { ByteArrayInputStream(bytes) }
            ),
            ResumeStatus(ResumeState.NONE)
        )
        try {
            val result = FileTransferClient().sendPrepared(
                InetAddress.getLoopbackAddress(), harness.port(), prepared,
                TransferStartMode.NEW, ResumeStatus(ResumeState.NONE),
                TransferPauseController(), {}, {}
            )
            assertTrue(result.exceptionOrNull()?.message, result.isSuccess)
            withTimeout(2_000) { harness.completed.await() }
            assertArrayEquals(bytes, harness.files.bytes(offer.transferId))
            assertTrue(harness.files.published)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `disconnect after durable ack remains available`() = runBlocking {
        val harness = V4Harness()
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE) { (it % 251).toByte() }
        val offer = harness.offer(bytes.size.toLong())
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, ChunkCodec.create(0, bytes))
                output.flush()
                assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
            }

            val status = harness.query(offer)
            assertEquals(ResumeState.AVAILABLE, status.state)
            assertEquals(bytes.size.toLong(), status.confirmedBytes)
            assertArrayEquals(bytes, harness.files.bytes(offer.transferId))
        } finally {
            harness.close()
        }
    }

    @Test
    fun `resume truncates bytes that were never checkpointed`() = runBlocking {
        val harness = V4Harness()
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE + 19) { (it % 251).toByte() }
        val offer = harness.offer(bytes.size.toLong())
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, ChunkCodec.create(0, bytes.copyOf(TransferProtocol.CHUNK_SIZE)))
                output.flush()
                assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
            }
            withTimeout(2_000) {
                while (!harness.store.isIdle(offer.transferId)) delay(10)
            }
            harness.files.append(offer.transferId, byteArrayOf(9, 9, 9, 9))
            val status = harness.query(offer)
            val prepared = PreparedTransfer(
                offer,
                SendFileSource(
                    "a.bin", "application/octet-stream", bytes.size.toLong(),
                    sourceUri = "content://source/a", openStream = { ByteArrayInputStream(bytes) }
                ),
                status
            )
            val result = FileTransferClient().sendPrepared(
                InetAddress.getLoopbackAddress(), harness.port(), prepared,
                TransferStartMode.RESUME, status, TransferPauseController(), {}, {}
            )
            assertTrue(result.exceptionOrNull()?.message, result.isSuccess)
            assertArrayEquals(bytes, harness.files.bytes(offer.transferId))
        } finally {
            harness.close()
        }
    }

    @Test
    fun `corrupted receiver prefix is rejected without publishing`() = runBlocking {
        val harness = V4Harness()
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE + 1) { (it % 251).toByte() }
        val offer = harness.offer(bytes.size.toLong())
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, ChunkCodec.create(0, bytes.copyOf(TransferProtocol.CHUNK_SIZE)))
                output.flush()
                assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
            }
            withTimeout(2_000) {
                while (!harness.store.isIdle(offer.transferId)) delay(10)
            }
            harness.files.corruptFirstByte(offer.transferId)
            val status = harness.query(offer)
            assertEquals(ResumeState.INVALID, status.state)
            assertFalse(harness.files.published)
            assertTrue(harness.store.findIncoming(offer.transferId) != null)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `zero byte transfer publishes only after final digest`() = runBlocking {
        val harness = V4Harness()
        val offer = harness.offer(0)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                output.writeByte(TransferProtocol.COMPLETE)
                output.write(MessageDigest.getInstance("SHA-256").digest())
                output.flush()
                assertEquals(TransferProtocol.SUCCESS, input.readUnsignedByte())
            }
            withTimeout(2_000) { harness.completed.await() }
            assertTrue(harness.files.published)
            assertEquals(ResumeState.COMPLETED, harness.query(offer).state)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `whole digest mismatch retains checkpoint and does not publish`() = runBlocking {
        val harness = V4Harness()
        val bytes = byteArrayOf(1, 2, 3)
        val offer = harness.offer(bytes.size.toLong())
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, ChunkCodec.create(0, bytes))
                output.flush()
                assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
                output.writeByte(TransferProtocol.COMPLETE)
                output.write(ByteArray(ChunkCodec.DIGEST_SIZE) { 9 })
                output.flush()
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
            assertFalse(harness.files.published)
            assertEquals(ResumeState.AVAILABLE, harness.query(offer).state)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `duplicate committed chunk is acked without a second write`() = runBlocking {
        val harness = V4Harness()
        val first = ByteArray(TransferProtocol.CHUNK_SIZE) { (it % 251).toByte() }
        val offer = harness.offer(first.size.toLong() + 1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                repeat(2) {
                    TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                    ChunkCodec.write(output, ChunkCodec.create(0, first))
                    output.flush()
                    assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
                }
                assertEquals(first.size.toLong(), harness.query(offer).confirmedBytes)
                assertArrayEquals(first, harness.files.bytes(offer.transferId))
            }
        } finally {
            harness.close()
        }
    }

    @Test
    fun `fourth consecutive bad chunk for same expected index is fatal`() = runBlocking {
        val harness = V4Harness(verifier = ChunkVerifier { _, _ -> false })
        val bytes = byteArrayOf(1)
        val offer = harness.offer(1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                repeat(4) { attempt ->
                    TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                    ChunkCodec.write(output, ChunkCodec.create(0, bytes))
                    output.flush()
                    assertEquals(
                        if (attempt < 3) TransferProtocol.NACK else TransferProtocol.FATAL,
                        input.readUnsignedByte()
                    )
                }
            }
        } finally {
            harness.close()
        }
    }

    @Test
    fun `fourth consecutive duplicate committed chunk is fatal without rewriting`() = runBlocking {
        val harness = V4Harness()
        val first = ByteArray(TransferProtocol.CHUNK_SIZE) { 2 }
        val offer = harness.offer(first.size.toLong() + 1)
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(output, ChunkCodec.create(0, first))
                output.flush()
                assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
                repeat(4) { attempt ->
                    TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                    ChunkCodec.write(output, ChunkCodec.create(0, first))
                    output.flush()
                    assertEquals(
                        if (attempt < 3) TransferProtocol.ACK else TransferProtocol.FATAL,
                        input.readUnsignedByte()
                    )
                }
            }
            assertEquals(1, harness.files.writeCount)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `lost final chunk ack retry is idempotent then completes`() = runBlocking {
        val harness = V4Harness()
        val bytes = byteArrayOf(7, 8, 9)
        val offer = harness.offer(bytes.size.toLong())
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertReady(input)
                repeat(2) {
                    TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                    ChunkCodec.write(output, ChunkCodec.create(0, bytes))
                    output.flush()
                    assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
                }
                output.writeByte(TransferProtocol.COMPLETE)
                output.write(MessageDigest.getInstance("SHA-256").digest(bytes))
                output.flush()
                assertEquals(TransferProtocol.SUCCESS, input.readUnsignedByte())
            }
            assertEquals(1, harness.files.writeCount)
            assertTrue(harness.files.published)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `resume query succeeds while transfer owns busy gate`() = runBlocking {
        val harness = V4Harness()
        val offer = harness.offer(1)
        var active: Socket? = null
        try {
            active = Socket(InetAddress.getLoopbackAddress(), harness.port())
            val output = DataOutputStream(BufferedOutputStream(active.getOutputStream()))
            start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
            assertReady(DataInputStream(BufferedInputStream(active.getInputStream())))
            withTimeout(2_000) {
                while (harness.store.findIncoming(offer.transferId) == null) delay(10)
            }

            val query = harness.query(offer)
            assertEquals(ResumeState.AVAILABLE, query.state)
            assertEquals(1, harness.startCalls)
        } finally {
            active?.close()
            harness.close()
        }
    }

    @Test
    fun `concurrent transfer start is rejected by busy gate`() = runBlocking {
        val harness = V4Harness(exclusiveBusyGate = true)
        val first = harness.offer(1)
        val second = harness.offer(1)
        var active: Socket? = null
        try {
            active = Socket(InetAddress.getLoopbackAddress(), harness.port())
            start(
                DataOutputStream(BufferedOutputStream(active.getOutputStream())),
                first, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE)
            )
            withTimeout(2_000) {
                while (harness.store.findIncoming(first.transferId) == null) delay(10)
            }
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                start(output, second, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
            assertEquals(2, harness.startCalls)
        } finally {
            active?.close()
            harness.close()
        }
    }

    @Test
    fun `raw v3 receives explicit fatal response`() = runBlocking {
        val harness = V4Harness()
        try {
            Socket(InetAddress.getLoopbackAddress(), harness.port()).use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                output.writeBytes("LTF3")
                output.writeByte(3)
                output.flush()
                assertEquals(TransferProtocol.FATAL, input.readUnsignedByte())
            }
        } finally {
            harness.close()
        }
    }

    @Test
    fun `local stop cancels history and retains resumable partial`() = runBlocking {
        val harness = V4Harness()
        val first = ByteArray(TransferProtocol.CHUNK_SIZE) { 4 }
        val offer = harness.offer(first.size.toLong() + 1)
        var socket: Socket? = null
        try {
            socket = Socket(InetAddress.getLoopbackAddress(), harness.port())
            val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
            val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            start(output, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
            assertReady(input)
            TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
            ChunkCodec.write(output, ChunkCodec.create(0, first))
            output.flush()
            assertEquals(TransferProtocol.ACK, input.readUnsignedByte())

            harness.stop()
            withTimeout(2_000) { harness.history.cancelled.await() }
            assertTrue(harness.store.findIncoming(offer.transferId) != null)
            assertArrayEquals(first, harness.files.bytes(offer.transferId))
        } finally {
            socket?.close()
            harness.close()
        }
    }

    private fun start(
        output: DataOutputStream,
        offer: TransferOffer,
        mode: TransferStartMode,
        status: ResumeStatus
    ) {
        ConnectionProtocol.writePreamble(output, ConnectionKind.TRANSFER_START)
        ResumeProtocol.writeOffer(output, offer)
        ResumeProtocol.writeStartMode(output, mode)
        ResumeProtocol.writeStatus(output, status)
        output.flush()
    }

    private fun assertReady(input: DataInputStream) {
        val response = ResumeProtocol.readStartResponse(input)
        assertEquals(com.example.transfer.protocol.TransferStartResponse.READY, response.response)
    }

    private fun sendChunk(
        output: DataOutputStream,
        input: DataInputStream,
        index: Int,
        bytes: ByteArray
    ) {
        TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
        ChunkCodec.write(output, ChunkCodec.create(index, bytes))
        output.flush()
        assertEquals(TransferProtocol.ACK, input.readUnsignedByte())
    }

    private fun pauseAndResume(
        output: DataOutputStream,
        input: DataInputStream,
        beforeResume: () -> Unit
    ) {
        TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
        output.flush()
        assertEquals(TransferProtocol.CONTROL_ACK, input.readUnsignedByte())
        beforeResume()
        TransferFrameCodec.writeType(output, TransferFrameType.RESUME)
        output.flush()
        assertEquals(TransferProtocol.CONTROL_ACK, input.readUnsignedByte())
    }
}

private class V4Harness(
    private val exclusiveBusyGate: Boolean = false,
    verifier: ChunkVerifier = ChunkVerifier.SHA256,
    private val configuredHistory: SocketHistory = SocketHistory(),
    pausedSessionLeaseMillis: Int = 30 * 60 * 1000,
    maxActiveConnections: Int = FileTransferServer.MAX_ACTIVE_CONNECTIONS,
    maxConcurrentQueries: Int = FileTransferServer.MAX_CONCURRENT_QUERIES,
    private val recordAttempts: Boolean = false,
    monotonicMillis: () -> Long = { System.nanoTime() / 1_000_000 }
) {
    val store = SocketResumeStore()
    val files = SocketFiles()
    val completed = CompletableDeferred<Unit>()
    val history = configuredHistory
    val errors = java.util.Collections.synchronizedList(mutableListOf<String>())
    val attemptEvents = java.util.Collections.synchronizedList(mutableListOf<Pair<String, Long>>())
    private val job = SupervisorJob()
    private val scope = CoroutineScope(job + Dispatchers.IO)
    private val ready = CompletableDeferred<Int>()
    private val stopping = CountDownLatch(1)
    var startCalls = 0
    private var busy = false
    private val server = FileTransferServer(
        port = 0,
        resumeCoordinator = ResumeCoordinator(store, files),
        chunkVerifier = verifier,
        pausedSessionLeaseMillis = pausedSessionLeaseMillis,
        monotonicMillis = monotonicMillis,
        onTransferAttemptStart = { attemptId ->
            if (recordAttempts) attemptEvents += "start" to attemptId
            startCalls++
            if (exclusiveBusyGate && busy) false else { busy = true; true }
        },
        onTransferAttemptEnd = { attemptId ->
            if (recordAttempts) attemptEvents += "end" to attemptId
            busy = false
        },
        history = history,
        maxActiveConnections = maxActiveConnections,
        maxConcurrentQueries = maxConcurrentQueries,
        onServerStopping = { stopping.countDown() }
    )

    init {
        server.start(
            scope,
            { ready.complete(it) },
            { attemptId, _, _ -> if (recordAttempts) attemptEvents += "progress" to attemptId },
            { attemptId, _ ->
                if (recordAttempts) attemptEvents += "complete" to attemptId
                completed.complete(Unit)
            },
            { attemptId, message ->
                if (recordAttempts && attemptId != null) attemptEvents += "error" to attemptId
                errors += message
            }
        )
    }

    suspend fun port() = withTimeout(2_000) { ready.await() }

    fun offer(size: Long) = TransferOffer(
        UUID.randomUUID().toString(), "sender", "a.bin", "application/octet-stream", size
    ).also { lastOfferId = it.transferId }

    var lastOfferId: String = ""
        private set

    fun prepared(bytes: ByteArray): PreparedTransfer {
        val offer = offer(bytes.size.toLong())
        return PreparedTransfer(
            offer,
            SendFileSource(
                "a.bin", "application/octet-stream", bytes.size.toLong(),
                sourceUri = "content://source/a", openStream = { ByteArrayInputStream(bytes) }
            ),
            ResumeStatus(ResumeState.NONE)
        )
    }

    suspend fun query(offer: TransferOffer): ResumeStatus =
        FileTransferClient().queryResume(InetAddress.getLoopbackAddress(), port(), offer)

    fun stop() = server.stop()

    fun awaitStopping() = stopping.await(2, TimeUnit.SECONDS)

    suspend fun close() {
        server.stop()
        withTimeout(2_000) { job.cancelAndJoin() }
    }
}

private class SocketHistory(private val failFailure: Exception? = null) : IncomingTransferHistory {
    val cancelled = CompletableDeferred<Unit>()
    val succeeded = CompletableDeferred<Unit>()
    val failed = CompletableDeferred<Unit>()
    override suspend fun start(fileName: String, fileSize: Long, mimeType: String, peerAddress: String) = 1L
    override suspend fun succeed(historyId: Long?, receivedUri: String?) { succeeded.complete(Unit) }
    override suspend fun fail(historyId: Long?, errorMessage: String?) {
        failed.complete(Unit)
        failFailure?.let { throw it }
    }
    override suspend fun cancel(historyId: Long?, errorMessage: String?) { cancelled.complete(Unit) }
}

private class SocketFiles : ResumableIncomingFileStore {
    private data class Entry(val name: String, var bytes: ByteArray = ByteArray(0))
    private val entries = linkedMapOf<StoredFileLocation, Entry>()
    var published = false
    var createCount = 0
    var writeCount = 0
    var publishCount = 0
    private var queryReadBarrier: Pair<CountDownLatch, CountDownLatch>? = null
    private var publishBarrier: Pair<CountDownLatch, CountDownLatch>? = null

    fun bytes(stagingIdPrefix: String): ByteArray = entries.entries.first {
        it.key.value.startsWith(stagingIdPrefix)
    }.value.bytes

    fun append(stagingIdPrefix: String, bytes: ByteArray) {
        val entry = entries.entries.first { it.key.value.startsWith(stagingIdPrefix) }.value
        entry.bytes += bytes
    }

    fun corruptFirstByte(stagingIdPrefix: String) {
        val entry = entries.entries.first { it.key.value.startsWith(stagingIdPrefix) }.value
        entry.bytes[0] = (entry.bytes[0].toInt() xor 0xff).toByte()
    }

    fun pauseFirstQueryRead(entered: CountDownLatch, release: CountDownLatch) {
        queryReadBarrier = entered to release
    }

    fun pausePublish(entered: CountDownLatch, release: CountDownLatch) {
        publishBarrier = entered to release
    }

    override suspend fun create(transferId: String, fileName: String, mimeType: String): ResumableFileHandle {
        createCount++
        val location = StoredFileLocation("TEST", transferId)
        val entry = Entry(fileName)
        entries[location] = entry
        return handle(location, entry)
    }

    override suspend fun reopen(location: StoredFileLocation, displayName: String): ResumableFileHandle? =
        entries[location]?.let { handle(location, it) }

    override suspend fun openInput(location: StoredFileLocation) = entries[location]?.bytes?.let { bytes ->
        val barrier = queryReadBarrier.also { queryReadBarrier = null }
        if (barrier == null) {
            ByteArrayInputStream(bytes)
        } else {
            object : ByteArrayInputStream(bytes) {
                private var first = true
                override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
                    if (first) {
                        first = false
                        barrier.first.countDown()
                        check(barrier.second.await(2, TimeUnit.SECONDS))
                    }
                    return super.read(buffer, offset, length)
                }
            }
        }
    }

    override suspend fun publish(handle: ResumableFileHandle): String {
        publishCount++
        published = true
        publishBarrier?.also { barrier ->
            barrier.first.countDown()
            check(barrier.second.await(2, TimeUnit.SECONDS))
            publishBarrier = null
        }
        return "content://received/${handle.displayName}"
    }

    override suspend fun delete(location: StoredFileLocation) { entries.remove(location) }

    private fun handle(location: StoredFileLocation, entry: Entry) = object : ResumableFileHandle {
        override val location = location
        override val displayName = entry.name
        override fun length() = entry.bytes.size.toLong()
        override fun writeAt(offset: Long, source: ByteArray, length: Int) {
            writeCount++
            val required = offset.toInt() + length
            if (entry.bytes.size < required) entry.bytes = entry.bytes.copyOf(required)
            source.copyInto(entry.bytes, offset.toInt(), 0, length)
        }
        override fun truncate(length: Long) { entry.bytes = entry.bytes.copyOf(length.toInt()) }
        override fun force() = Unit
        override fun close() = Unit
    }
}

private class SocketResumeStore : ResumeStore {
    private val incoming = linkedMapOf<String, IncomingCheckpoint>()
    private val journals = linkedMapOf<String, IncomingStagingJournal>()
    private val receipts = linkedMapOf<String, CompletedReceipt>()

    fun isIdle(transferId: String) = synchronized(this) {
        incoming[transferId]?.operationState == IncomingOperationState.IDLE
    }

    override suspend fun findIncoming(transferId: String) = synchronized(this) { incoming[transferId] }
    override suspend fun saveIncoming(checkpoint: IncomingCheckpoint) { synchronized(this) { incoming[checkpoint.transferId] = checkpoint } }
    override suspend fun insertIncoming(checkpoint: IncomingCheckpoint) = synchronized(this) {
        if (incoming.containsKey(checkpoint.transferId)) false else { incoming[checkpoint.transferId] = checkpoint; true }
    }
    override suspend fun acquireIncomingSession(expected: IncomingCheckpoint, token: String, now: Long, expiresAt: Long) = synchronized(this) {
        val current = incoming[expected.transferId]
        if (current == null || current.sessionToken != null || current.generation != expected.generation ||
            current.nextChunkIndex != expected.nextChunkIndex) null else current.copy(
            sessionToken = token, sessionClaimedAt = now, operationState = IncomingOperationState.ACTIVE,
            updatedAt = now, expiresAt = expiresAt
        ).also { incoming[it.transferId] = it }
    }
    override suspend fun releaseIncomingSession(expected: IncomingCheckpoint, token: String) = synchronized(this) {
        val current = incoming[expected.transferId]
        if (current?.sessionToken != token || current.generation != expected.generation ||
            current.nextChunkIndex != expected.nextChunkIndex) false else {
            incoming[current.transferId] = current.copy(sessionToken = null, sessionClaimedAt = null, operationState = IncomingOperationState.IDLE); true
        }
    }
    override suspend fun heartbeatIncomingSession(expected: IncomingCheckpoint, token: String, now: Long, expiresAt: Long) = synchronized(this) {
        val current = incoming[expected.transferId]
        if (current?.sessionToken != token || current.generation != expected.generation ||
            current.nextChunkIndex != expected.nextChunkIndex || current.operationState != IncomingOperationState.ACTIVE) false else {
            incoming[current.transferId] = current.copy(sessionClaimedAt = now, updatedAt = now, expiresAt = expiresAt); true
        }
    }
    override suspend fun commitOwnedIncomingChunk(expected: IncomingCheckpoint, token: String, confirmedBytes: Long, nextChunkIndex: Int, chainDigest: ByteArray, lastChunkHash: ByteArray, updatedAt: Long, expiresAt: Long) = synchronized(this) {
        val current = incoming[expected.transferId]
        if (current?.sessionToken != token || current.generation != expected.generation || current.nextChunkIndex != expected.nextChunkIndex) false else {
            incoming[current.transferId] = current.copy(confirmedBytes = confirmedBytes, nextChunkIndex = nextChunkIndex,
                chainDigest = chainDigest, lastChunkHash = lastChunkHash, updatedAt = updatedAt, expiresAt = expiresAt); true
        }
    }
    override suspend fun beginIncomingCompletion(expected: IncomingCheckpoint, token: String, finalDigest: ByteArray, now: Long, expiresAt: Long) = synchronized(this) {
        val current = incoming[expected.transferId]
        if (current?.sessionToken != token || current.generation != expected.generation || current.nextChunkIndex != expected.nextChunkIndex) false else {
            incoming[current.transferId] = current.copy(
                operationState = IncomingOperationState.COMPLETING,
                completingFinalDigest = finalDigest
            ); true
        }
    }
    override suspend fun deleteCompletingIncoming(expected: IncomingCheckpoint) = synchronized(this) {
        val current = incoming[expected.transferId]
        if (current?.operationState != IncomingOperationState.COMPLETING) false else { incoming.remove(expected.transferId); true }
    }
    override suspend fun findCompletedReceipt(transferId: String, now: Long) = synchronized(this) {
        val receipt = receipts[transferId]
        if (receipt != null && receipt.expiresAt <= now) {
            receipts.remove(transferId)
            null
        } else receipt
    }
    override suspend fun finishIncomingCompletion(expected: IncomingCheckpoint, receipt: CompletedReceipt) = synchronized(this) {
        val current = incoming[expected.transferId]
        if (current?.operationState != IncomingOperationState.COMPLETING ||
            current.completingFinalDigest?.contentEquals(receipt.finalDigest) != true
        ) false else {
            receipts[receipt.transferId] = receipt
            incoming.remove(expected.transferId)
            true
        }
    }
    override suspend fun insertStagingJournal(journal: IncomingStagingJournal) = synchronized(this) {
        if (journals.containsKey(journal.transferId)) false else { journals[journal.transferId] = journal; true }
    }
    override suspend fun deleteStagingJournal(journal: IncomingStagingJournal) = synchronized(this) { journals.remove(journal.transferId, journal) }
    override suspend fun commitIncomingChunk(transferId: String, expectedNextChunkIndex: Int, confirmedBytes: Long, nextChunkIndex: Int, chainDigest: ByteArray, lastChunkHash: ByteArray, updatedAt: Long, expiresAt: Long) = false
    override suspend fun updateIncomingExpiry(transferId: String, updatedAt: Long, expiresAt: Long) = false
    override suspend fun deleteIncoming(transferId: String) { synchronized(this) { incoming.remove(transferId) } }
    override suspend fun findOutgoing(sourceUri: String, peerDeviceId: String): OutgoingResumeLink? = null
    override suspend fun saveOutgoing(link: OutgoingResumeLink) = Unit
    override suspend fun updateOutgoingTimestamp(transferId: String, updatedAt: Long) = false
    override suspend fun deleteOutgoing(transferId: String) = Unit
    override suspend fun deleteCompleted(transferId: String) = Unit
    override suspend fun claimExpiredIncoming(now: Long, staleClaimBefore: Long, token: String) = emptyList<IncomingCheckpoint>()
    override suspend fun deleteClaimedIncoming(token: String) = 0
    override suspend fun releaseClaimedIncoming(token: String) = 0
    override suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long) = 0
}
