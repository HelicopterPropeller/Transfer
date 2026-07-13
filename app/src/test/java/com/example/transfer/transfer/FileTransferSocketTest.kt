package com.example.transfer.transfer

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

class FileTransferSocketTest {
    @Test
    fun `client and server stream a file and acknowledge completion`() = runBlocking {
        val bytes = ByteArray(180_000) { (it % 251).toByte() }
        val store = MemoryStore()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val ready = CompletableDeferred<Int>()
        val received = CompletableDeferred<String>()
        val server = FileTransferServer(0, store)
        server.start(
            scope = scope,
            onStarted = { ready.complete(it) },
            onProgress = { _, _ -> },
            onComplete = { received.complete(it) },
            onError = { received.completeExceptionally(AssertionError(it)) }
        )

        val sendProgress = mutableListOf<Int>()
        val result = FileTransferClient().send(
            InetAddress.getLoopbackAddress(),
            withTimeout(3_000) { ready.await() },
            SendFileSource("sample.bin", "application/octet-stream", bytes.size.toLong()) {
                ByteArrayInputStream(bytes)
            }
        ) { sendProgress += it }

        assertTrue(result.isSuccess)
        assertEquals("sample.bin", withTimeout(3_000) { received.await() })
        assertArrayEquals(bytes, store.bytes.toByteArray())
        assertEquals(100, sendProgress.last())
        assertTrue(store.completed)

        server.stop()
        scope.cancel()
    }

    private class MemoryStore : IncomingFileStore {
        val bytes = ByteArrayOutputStream()
        var completed = false

        override suspend fun create(fileName: String, mimeType: String) =
            ReceivedFileHandle(bytes, displayName = fileName)

        override suspend fun complete(handle: ReceivedFileHandle) {
            completed = true
        }

        override suspend fun abort(handle: ReceivedFileHandle) {
            bytes.reset()
        }
    }
}
