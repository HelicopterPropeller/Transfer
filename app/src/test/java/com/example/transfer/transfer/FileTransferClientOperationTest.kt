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
import com.example.transfer.protocol.TransferOffer
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.resume.PreparedTransfer
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.SocketTimeoutException
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

class FileTransferClientOperationTest {
    @Test
    fun `resume query timeout scales conservatively with offered file size`() {
        assertEquals(15_000, FileTransferClient.queryReadTimeoutMillis(0))
        assertTrue(
            FileTransferClient.queryReadTimeoutMillis(TransferProtocol.MAX_FILE_SIZE) >
                FileTransferClient.queryReadTimeoutMillis(TransferProtocol.CHUNK_SIZE.toLong())
        )
    }

    @Test
    fun `resume query timeout safely bounds invalid and extreme file sizes`() {
        org.junit.Assert.assertThrows(IllegalArgumentException::class.java) {
            FileTransferClient.queryReadTimeoutMillis(-1)
        }
        val protocolMaximum = FileTransferClient.queryReadTimeoutMillis(
            TransferProtocol.MAX_FILE_SIZE
        )

        assertEquals(40_975_000, protocolMaximum)
        assertEquals(protocolMaximum, FileTransferClient.queryReadTimeoutMillis(Long.MAX_VALUE))
        assertTrue(protocolMaximum <= Int.MAX_VALUE)
    }

    @Test
    fun `cancel active closes a source blocked during resumed prefix scan`() = runBlocking {
        val prefix = ByteArray(TransferProtocol.CHUNK_SIZE) { (it % 251).toByte() }
        val bytes = prefix + byteArrayOf(9)
        val offer = offer(bytes.size.toLong())
        val expectedStatus = prefixStatus(prefix)
        val source = BlockingInputStream(bytes)
        val server = ServerSocket(0).apply { soTimeout = 1_500 }
        val serverJob = async(Dispatchers.IO) {
            try {
                server.accept().use { socket -> socket.getInputStream().read() }
                true
            } catch (_: SocketTimeoutException) {
                false
            }
        }
        val client = FileTransferClient()
        val send = async(Dispatchers.IO) {
            client.sendPrepared(
                InetAddress.getLoopbackAddress(), server.localPort,
                prepared(offer) { source }, TransferStartMode.RESUME, expectedStatus,
                TransferPauseController(), {}, {}
            )
        }

        withTimeout(2_000) { source.readStarted.await() }
        client.cancelActive()
        val completedByCancel = withTimeoutOrNull(500) { send.await() }
        if (completedByCancel == null) source.release()
        withTimeout(3_000) { send.await() }
        val connected = serverJob.await()

        assertNotNull("prefix scan must stop without external release", completedByCancel)
        assertTrue(source.wasClosed.get())
        assertEquals(false, connected)
        server.close()
    }

    @Test
    fun `completed query releases ownership for following transfer`() = runBlocking {
        val bytes = byteArrayOf(1, 2)
        val offer = offer(bytes.size.toLong())
        val status = ResumeStatus(ResumeState.NONE)
        val server = ServerSocket(0)
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertEquals(ConnectionKind.RESUME_QUERY, ConnectionProtocol.readPreamble(input))
                ResumeProtocol.readOffer(input)
                ResumeProtocol.writeStatus(output, status)
                output.flush()
                assertEquals(-1, input.read())
            }
            server.accept().use { socket -> completeNewTransfer(socket.getInputStream(), socket.getOutputStream(), offer, bytes) }
        }
        val client = FileTransferClient()

        assertStatus(status, client.queryResume(InetAddress.getLoopbackAddress(), server.localPort, offer))
        val result = client.sendPrepared(
            InetAddress.getLoopbackAddress(), server.localPort, prepared(offer) { ByteArrayInputStream(bytes) },
            TransferStartMode.NEW, status, TransferPauseController(), {}, {}
        )

        assertTrue(result.isSuccess)
        serverJob.await()
        server.close()
    }

    @Test
    fun `concurrent transfer registration is rejected without replacing active query`() = runBlocking {
        val bytes = byteArrayOf(5)
        val offer = offer(bytes.size.toLong())
        val status = ResumeStatus(ResumeState.NONE)
        val queryServer = ServerSocket(0)
        val transferServer = ServerSocket(0).apply { soTimeout = 1_000 }
        val queryAccepted = CompletableDeferred<Unit>()
        val releaseQuery = CompletableDeferred<Unit>()
        val queryServerJob = async(Dispatchers.IO) {
            queryServer.accept().use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                ConnectionProtocol.readPreamble(input)
                ResumeProtocol.readOffer(input)
                queryAccepted.complete(Unit)
                releaseQuery.await()
                ResumeProtocol.writeStatus(output, status)
                output.flush()
            }
        }
        val transferServerJob = async(Dispatchers.IO) {
            try {
                transferServer.accept().use { socket ->
                    completeNewTransfer(socket.getInputStream(), socket.getOutputStream(), offer, bytes)
                }
                true
            } catch (_: SocketTimeoutException) {
                false
            }
        }
        val client = FileTransferClient()
        val query = async(Dispatchers.IO) {
            client.queryResume(InetAddress.getLoopbackAddress(), queryServer.localPort, offer)
        }
        withTimeout(2_000) { queryAccepted.await() }

        val transfer = client.sendPrepared(
            InetAddress.getLoopbackAddress(), transferServer.localPort,
            prepared(offer) { ByteArrayInputStream(bytes) }, TransferStartMode.NEW, status,
            TransferPauseController(), {}, {}
        )
        releaseQuery.complete(Unit)

        assertTrue(transfer.isFailure)
        assertStatus(status, withTimeout(2_000) { query.await() })
        assertEquals(false, transferServerJob.await())
        queryServerJob.await()
        queryServer.close()
        transferServer.close()
    }

    @Test
    fun `cancelled operation cleanup cannot clear a replacement query`() = runBlocking {
        val offer = offer(0)
        val firstServer = ServerSocket(0)
        val secondServer = ServerSocket(0)
        val firstAccepted = CompletableDeferred<Unit>()
        val secondAccepted = CompletableDeferred<Unit>()
        val firstServerJob = async(Dispatchers.IO) {
            firstServer.accept().use { socket ->
                firstAccepted.complete(Unit)
                socket.getInputStream().read()
            }
        }
        val secondServerJob = async(Dispatchers.IO) {
            secondServer.accept().use { socket ->
                secondAccepted.complete(Unit)
                socket.getInputStream().read()
            }
        }
        val client = FileTransferClient()
        val first = async(Dispatchers.IO) {
            runCatching { client.queryResume(InetAddress.getLoopbackAddress(), firstServer.localPort, offer) }
        }
        withTimeout(2_000) { firstAccepted.await() }
        client.cancelActive()
        val second = async(Dispatchers.IO) {
            runCatching { client.queryResume(InetAddress.getLoopbackAddress(), secondServer.localPort, offer) }
        }
        withTimeout(2_000) { secondAccepted.await() }
        withTimeout(2_000) { first.await() }

        client.cancelActive()

        assertTrue(withTimeout(2_000) { second.await() }.isFailure)
        firstServer.close()
        secondServer.close()
        firstServerJob.await()
        secondServerJob.await()
        Unit
    }

    @Test
    fun `cancel active wakes v4 transfer paused after acknowledged chunk`() = runBlocking {
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE + 1) { (it % 251).toByte() }
        val offer = offer(bytes.size.toLong())
        val status = ResumeStatus(ResumeState.NONE)
        val server = ServerSocket(0)
        val pauseAcknowledged = CompletableDeferred<Unit>()
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertEquals(ConnectionKind.TRANSFER_START, ConnectionProtocol.readPreamble(input))
                ResumeProtocol.readOffer(input)
                ResumeProtocol.readStartMode(input)
                ResumeProtocol.readStatus(input, offer)
                assertEquals(TransferFrameType.CHUNK, TransferFrameCodec.readType(input))
                ChunkCodec.read(input, 0, TransferProtocol.CHUNK_SIZE)
                output.writeByte(TransferProtocol.ACK)
                output.flush()
                assertEquals(TransferFrameType.PAUSE, TransferFrameCodec.readType(input))
                output.writeByte(TransferProtocol.CONTROL_ACK)
                output.flush()
                pauseAcknowledged.complete(Unit)
                input.read()
            }
        }
        val client = FileTransferClient()
        val controller = TransferPauseController()
        val paused = CompletableDeferred<Unit>()
        val send = async(Dispatchers.IO) {
            client.sendPrepared(
                InetAddress.getLoopbackAddress(), server.localPort,
                prepared(offer) { ByteArrayInputStream(bytes) }, TransferStartMode.NEW, status,
                controller,
                { if (it == TransferPauseState.PAUSED) paused.complete(Unit) },
                { if (it.confirmedBytes == TransferProtocol.CHUNK_SIZE.toLong()) controller.requestPause() }
            )
        }
        withTimeout(3_000) {
            pauseAcknowledged.await()
            paused.await()
        }

        client.cancelActive()

        assertTrue(withTimeout(2_000) { send.await() }.isFailure)
        assertEquals(TransferPauseState.CANCELLED, controller.state)
        serverJob.await()
        server.close()
    }

    private fun completeNewTransfer(
        rawInput: InputStream,
        rawOutput: java.io.OutputStream,
        offer: TransferOffer,
        bytes: ByteArray
    ) {
        val input = DataInputStream(rawInput)
        val output = DataOutputStream(rawOutput)
        assertEquals(ConnectionKind.TRANSFER_START, ConnectionProtocol.readPreamble(input))
        assertEquals(offer, ResumeProtocol.readOffer(input))
        assertEquals(TransferStartMode.NEW, ResumeProtocol.readStartMode(input))
        ResumeProtocol.readStatus(input, offer)
        assertEquals(TransferFrameType.CHUNK, TransferFrameCodec.readType(input))
        ChunkCodec.read(input, 0, bytes.size)
        output.writeByte(TransferProtocol.ACK)
        output.flush()
        assertEquals(TransferProtocol.COMPLETE, input.readUnsignedByte())
        input.readFully(ByteArray(ChunkCodec.DIGEST_SIZE))
        output.writeByte(TransferProtocol.SUCCESS)
        output.flush()
    }

    private fun prepared(offer: TransferOffer, open: () -> InputStream) = PreparedTransfer(
        offer,
        SendFileSource(
            offer.fileName, offer.mimeType, offer.fileSize,
            "content://test/${offer.transferId}", 1L, open
        ),
        ResumeStatus(ResumeState.NONE)
    )

    private fun offer(size: Long) = TransferOffer(
        UUID.randomUUID().toString(), "sender-1", "file.bin", "application/octet-stream", size
    )

    private fun prefixStatus(bytes: ByteArray): ResumeStatus {
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(bytes), bytes.size.toLong())
        return ResumeStatus(
            ResumeState.AVAILABLE, prefix.scannedBytes, prefix.nextChunkIndex,
            prefix.chainDigest, prefix.lastChunkHash
        )
    }

    private fun assertStatus(expected: ResumeStatus, actual: ResumeStatus) {
        assertEquals(expected.state, actual.state)
        assertEquals(expected.confirmedBytes, actual.confirmedBytes)
        assertEquals(expected.nextChunkIndex, actual.nextChunkIndex)
    }

    private class BlockingInputStream(bytes: ByteArray) : InputStream() {
        private val delegate = ByteArrayInputStream(bytes)
        private val gate = CountDownLatch(1)
        private val started = AtomicBoolean()
        val readStarted = CompletableDeferred<Unit>()
        val wasClosed = AtomicBoolean()

        override fun read(): Int {
            awaitFirstRead()
            if (wasClosed.get()) throw IOException("closed")
            return delegate.read()
        }

        override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
            awaitFirstRead()
            if (wasClosed.get()) throw IOException("closed")
            return delegate.read(buffer, offset, length)
        }

        override fun close() {
            wasClosed.set(true)
            gate.countDown()
            delegate.close()
        }

        fun release() = gate.countDown()

        private fun awaitFirstRead() {
            if (started.compareAndSet(false, true)) readStarted.complete(Unit)
            gate.await()
        }
    }
}
