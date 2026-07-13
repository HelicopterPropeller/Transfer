package com.example.transfer.transfer

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.storage.IncomingFileStore
import com.example.transfer.storage.ReceivedFileHandle
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

class FileTransferSocketTest {
    @Test
    fun `streams multiple verified chunks`() = runBlocking {
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE * 2 + 123) { (it % 251).toByte() }
        val result = transfer(bytes)
        assertTrue(result.sendResult.isSuccess)
        assertArrayEquals(bytes, result.store.bytes.toByteArray())
        assertEquals(100, result.progress.last())
        assertTrue(result.store.completed)
        result.close()
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
        assertEquals(listOf(100), result.progress)
        result.close()
    }

    @Test
    fun `three nacks fail and abort destination`() = runBlocking {
        val checks = AtomicInteger()
        val result = transfer(ByteArray(64) { 7 }, ChunkVerifier { _, _ -> checks.incrementAndGet(); false })
        assertTrue(result.sendResult.isFailure)
        assertEquals(3, checks.get())
        assertTrue(result.store.aborted)
        result.close()
    }

    private suspend fun transfer(bytes: ByteArray, verifier: ChunkVerifier = ChunkVerifier.SHA256): TransferResult {
        val store = MemoryStore()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val ready = CompletableDeferred<Int>()
        val server = FileTransferServer(0, store, verifier)
        server.start(scope, { ready.complete(it) }, { _, _ -> }, {}, {})
        val progress = mutableListOf<Int>()
        val sendResult = FileTransferClient().send(
            InetAddress.getLoopbackAddress(),
            withTimeout(3_000) { ready.await() },
            SendFileSource("sample.bin", "application/octet-stream", bytes.size.toLong()) {
                ByteArrayInputStream(bytes)
            }
        ) { progress += it }
        return TransferResult(store, progress, sendResult, server, scope)
    }

    private data class TransferResult(
        val store: MemoryStore,
        val progress: List<Int>,
        val sendResult: Result<Unit>,
        val server: FileTransferServer,
        val scope: CoroutineScope
    ) {
        fun close() {
            server.stop()
            scope.cancel()
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
