package com.example.transfer.transfer

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.TransferFrameCodec
import com.example.transfer.protocol.TransferFrameType
import com.example.transfer.protocol.TransferHeader
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.storage.IncomingFileStore
import com.example.transfer.storage.ReceivedFileHandle
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetAddress
import java.net.Socket
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

class FileTransferSocketTest {
    @Test
    fun `pause lease must be positive`() {
        org.junit.Assert.assertThrows(IllegalArgumentException::class.java) {
            FileTransferServer(
                port = 0,
                store = MemoryStore(),
                pausedSessionLeaseMillis = 0
            )
        }
    }

    @Test
    fun `pause lease aborts when resume never arrives`() = runBlocking {
        val server = RawServerHarness(
            serverFactory = {
                FileTransferServer(
                    port = 0,
                    store = it,
                    pausedSessionLeaseMillis = 100
                )
            }
        )
        try {
            val response = Socket(InetAddress.getLoopbackAddress(), server.port()).use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                TransferProtocol.writeHeader(
                    output,
                    TransferHeader(
                        "raw.bin",
                        "application/octet-stream",
                        TransferProtocol.CHUNK_SIZE.toLong() + 1
                    )
                )
                TransferFrameCodec.writeType(output, TransferFrameType.CHUNK)
                ChunkCodec.write(
                    output,
                    ChunkCodec.create(0, ByteArray(TransferProtocol.CHUNK_SIZE))
                )
                output.flush()
                assertEquals(TransferProtocol.ACK, input.readUnsignedByte())

                TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                output.flush()
                assertEquals(TransferProtocol.CONTROL_ACK, input.readUnsignedByte())
                input.readUnsignedByte()
            }

            withTimeout(2_000) { server.terminal.await() }
            assertEquals(TransferProtocol.FATAL, response)
            assertEquals(listOf("Paused session lease expired"), server.errors())
            assertTrue(server.store.aborted)
        } finally {
            server.close()
        }
    }

    @Test
    fun `pause before first verified chunk ack is rejected`() = runBlocking {
        val server = RawServerHarness()
        try {
            val response = Socket(InetAddress.getLoopbackAddress(), server.port()).use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                TransferProtocol.writeHeader(
                    output,
                    TransferHeader("raw.bin", "application/octet-stream", 1)
                )
                TransferFrameCodec.writeType(output, TransferFrameType.PAUSE)
                output.flush()
                input.readUnsignedByte()
            }

            withTimeout(2_000) { server.terminal.await() }
            assertEquals(TransferProtocol.FATAL, response)
            assertEquals(listOf("Pause before first verified chunk ACK"), server.errors())
            assertTrue(server.store.aborted)
        } finally {
            server.close()
        }
    }

    @Test
    fun `streams multiple verified chunks`() = runBlocking {
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE * 2 + 123) { (it % 251).toByte() }
        val result = transfer(bytes)
        assertTrue(result.sendResult.isSuccess)
        assertArrayEquals(bytes, result.store.bytes.toByteArray())
        assertEquals(100, result.progress.last().percent)
        assertTrue(result.store.completed)
        result.assertNoServerErrors()
    }

    @Test
    fun `nack retries the same chunk and then succeeds`() = runBlocking {
        val checks = AtomicInteger()
        val verifier = ChunkVerifier { data, digest ->
            checks.incrementAndGet() > 1 && ChunkCodec.sha256(data).contentEquals(digest)
        }
        val result = transfer(ByteArray(1024) { it.toByte() }, verifier)
        assertTrue(result.sendResult.isSuccess)
        assertEquals(2, checks.get())
        assertEquals(listOf(FileTransferProgress(1024, 1024)), result.progress)
        result.assertNoServerErrors()
    }

    @Test
    fun `three nacks fail and abort destination`() = runBlocking {
        val checks = AtomicInteger()
        val result = transfer(ByteArray(64) { 7 }, ChunkVerifier { _, _ -> checks.incrementAndGet(); false })
        assertTrue(result.sendResult.isFailure)
        assertEquals(3, checks.get())
        assertTrue(result.store.aborted)
        assertEquals(listOf("Chunk 0 failed verification"), result.serverErrors)
    }

    @Test
    fun `server failure is exposed by transfer result`() = runBlocking {
        val result = transfer(
            ByteArray(64) { 7 },
            ChunkVerifier { _, _ -> error("synthetic server failure") }
        )

        assertTrue(result.sendResult.isFailure)
        assertEquals(listOf("synthetic server failure"), result.serverErrors)
    }

    @Test
    fun `zero byte file publishes completed byte progress`() = runBlocking {
        val result = transfer(ByteArray(0))

        assertTrue(result.sendResult.isSuccess)
        assertEquals(listOf(FileTransferProgress(0, 0)), result.progress)
        assertTrue(result.store.completed)
        result.assertNoServerErrors()
    }

    @Test
    fun `pause waits after verified chunk and resume completes remaining chunks`() = runBlocking {
        val firstChunk = CompletableDeferred<Unit>()
        val paused = CompletableDeferred<Unit>()
        val controller = TransferPauseController()
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE * 2 + 10) { (it % 251).toByte() }
        val transfer = async(Dispatchers.IO) {
            transfer(
                bytes = bytes,
                controller = controller,
                onProgress = { progress ->
                    if (progress.confirmedBytes == TransferProtocol.CHUNK_SIZE.toLong()) {
                        controller.requestPause()
                        firstChunk.complete(Unit)
                    }
                },
                onPauseState = { state ->
                    if (state == TransferPauseState.PAUSED) paused.complete(Unit)
                }
            )
        }

        withTimeout(3_000) {
            firstChunk.await()
            paused.await()
        }
        assertFalse(transfer.isCompleted)
        controller.requestResume()
        val result = withTimeout(5_000) { transfer.await() }
        assertTrue(result.sendResult.isSuccess)
        assertArrayEquals(bytes, result.store.bytes.toByteArray())
        result.assertNoServerErrors()
    }

    @Test
    fun `pause requested after final chunk completes file and remains pending`() = runBlocking {
        val controller = TransferPauseController()
        val bytes = ByteArray(1024) { (it % 251).toByte() }

        val result = transfer(
            bytes = bytes,
            controller = controller,
            onProgress = { progress ->
                if (progress.confirmedBytes == progress.totalBytes) controller.requestPause()
            }
        )

        assertTrue(result.sendResult.exceptionOrNull()?.message, result.sendResult.isSuccess)
        assertArrayEquals(bytes, result.store.bytes.toByteArray())
        assertTrue(result.store.completed)
        assertEquals(TransferPauseState.PAUSING, controller.state)
        result.assertNoServerErrors()
    }

    private suspend fun transfer(
        bytes: ByteArray,
        verifier: ChunkVerifier = ChunkVerifier.SHA256,
        controller: TransferPauseController = TransferPauseController(),
        onProgress: (FileTransferProgress) -> Unit = {},
        onPauseState: (TransferPauseState) -> Unit = {}
    ): TransferResult {
        val store = MemoryStore()
        val scopeJob = SupervisorJob()
        val scope = CoroutineScope(scopeJob + Dispatchers.IO)
        val ready = CompletableDeferred<Int>()
        val terminal = CompletableDeferred<Unit>()
        val serverErrors = Collections.synchronizedList(mutableListOf<String>())
        val server = FileTransferServer(0, store, verifier)
        server.start(
            scope,
            { ready.complete(it) },
            { _, _ -> },
            { terminal.complete(Unit) },
            { error ->
                serverErrors += error
                terminal.complete(Unit)
            }
        )
        val progress = mutableListOf<FileTransferProgress>()
        try {
            val sendResult = FileTransferClient().send(
                InetAddress.getLoopbackAddress(),
                withTimeout(3_000) { ready.await() },
                SendFileSource("sample.bin", "application/octet-stream", bytes.size.toLong()) {
                    ByteArrayInputStream(bytes)
                },
                controller,
                onPauseState
            ) {
                progress += it
                onProgress(it)
            }
            withTimeout(3_000) { terminal.await() }
            return TransferResult(
                store,
                progress,
                sendResult,
                synchronized(serverErrors) { serverErrors.toList() }
            )
        } finally {
            server.stop()
            withTimeout(3_000) { scopeJob.cancelAndJoin() }
        }
    }

    private data class TransferResult(
        val store: MemoryStore,
        val progress: List<FileTransferProgress>,
        val sendResult: Result<Unit>,
        val serverErrors: List<String>
    ) {
        fun assertNoServerErrors() = assertEquals(emptyList<String>(), serverErrors)
    }

    private class RawServerHarness(
        val store: MemoryStore = MemoryStore(),
        serverFactory: (MemoryStore) -> FileTransferServer = { FileTransferServer(0, it) }
    ) {
        private val scopeJob = SupervisorJob()
        private val scope = CoroutineScope(scopeJob + Dispatchers.IO)
        private val ready = CompletableDeferred<Int>()
        val terminal = CompletableDeferred<Unit>()
        private val serverErrors = Collections.synchronizedList(mutableListOf<String>())
        private val server = serverFactory(store)

        init {
            server.start(
                scope,
                { ready.complete(it) },
                { _, _ -> },
                { terminal.complete(Unit) },
                { error ->
                    serverErrors += error
                    terminal.complete(Unit)
                }
            )
        }

        suspend fun port(): Int = withTimeout(2_000) { ready.await() }

        fun errors(): List<String> = synchronized(serverErrors) { serverErrors.toList() }

        suspend fun close() {
            server.stop()
            withTimeout(2_000) { scopeJob.cancelAndJoin() }
        }
    }

    private class MemoryStore : IncomingFileStore {
        val bytes = ByteArrayOutputStream()
        var completed = false
        var aborted = false

        override suspend fun create(fileName: String, mimeType: String) =
            ReceivedFileHandle(bytes, displayName = fileName)

        override suspend fun complete(handle: ReceivedFileHandle) { completed = true }
        override suspend fun abort(handle: ReceivedFileHandle) { aborted = true; bytes.reset() }
    }
}
