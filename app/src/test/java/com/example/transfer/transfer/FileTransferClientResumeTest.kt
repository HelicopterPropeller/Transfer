package com.example.transfer.transfer

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.PrefixDigestScanner
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
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.SocketTimeoutException
import java.security.MessageDigest
import java.util.UUID
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class FileTransferClientResumeTest {
    @Test
    fun `query reports an explicit incompatibility when a v3 receiver rejects the v4 preamble`() = runBlocking {
        val offer = offer(fileSize = 1)
        val server = ServerSocket(0)
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                input.readInt() // A v3 receiver mistakes the LTF4 magic for its header length.
                output.writeByte(TransferProtocol.FATAL)
                output.flush()
            }
        }

        val error = runCatching {
            FileTransferClient().queryResume(
                InetAddress.getLoopbackAddress(), server.localPort, offer
            )
        }.exceptionOrNull()

        assertTrue(error is ProtocolException)
        assertEquals("对端协议版本不兼容，请将两台设备都升级到最新版", error?.message)
        serverJob.await()
        server.close()
    }

    @Test
    fun `client sends no chunk until receiver replies ready`() = runBlocking {
        val bytes = byteArrayOf(4, 5, 6)
        val offer = offer(bytes.size.toLong())
        val server = ServerSocket(0)
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertStart(input, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                socket.soTimeout = 250
                assertThrowsSocketTimeout { input.readUnsignedByte() }
                ResumeProtocol.writeStartResponse(output, TransferStartResponse.READY)
                output.flush()
                socket.soTimeout = 3_000
                assertEquals(TransferFrameType.CHUNK, TransferFrameCodec.readType(input))
                ChunkCodec.read(input, 0, bytes.size)
                output.writeByte(TransferProtocol.ACK)
                output.flush()
                assertEquals(TransferProtocol.COMPLETE, input.readUnsignedByte())
                ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully)
                output.writeByte(TransferProtocol.SUCCESS)
                output.flush()
            }
        }

        val result = FileTransferClient().sendPrepared(
            InetAddress.getLoopbackAddress(), server.localPort, prepared(offer, bytes),
            TransferStartMode.NEW, ResumeStatus(ResumeState.NONE), TransferPauseController(), {}, {}
        )

        assertTrue(result.isSuccess)
        serverJob.await()
        server.close()
    }

    @Test
    fun `completed status verifies local whole digest without opening transfer socket`() = runBlocking {
        val bytes = byteArrayOf(7, 8, 9)
        val offer = offer(bytes.size.toLong())
        val server = ServerSocket(0).apply { soTimeout = 400 }
        val digest = sha256(bytes)

        val result = FileTransferClient().sendPrepared(
            InetAddress.getLoopbackAddress(), server.localPort, prepared(offer, bytes),
            TransferStartMode.RESUME,
            ResumeStatus(ResumeState.COMPLETED, finalDigest = digest),
            TransferPauseController(), {}, {}
        )

        assertTrue(result.isSuccess)
        assertThrowsSocketTimeout { server.accept() }
        server.close()
    }

    @Test
    fun `completed start response verifies digest and sends no chunk`() = runBlocking {
        val bytes = byteArrayOf(1, 3, 5)
        val offer = offer(bytes.size.toLong())
        val server = ServerSocket(0)
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertStart(input, offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
                ResumeProtocol.writeStartResponse(output, TransferStartResponse.COMPLETED, sha256(bytes))
                output.flush()
                socket.soTimeout = 1_000
                assertEquals(-1, input.read())
            }
        }

        val result = FileTransferClient().sendPrepared(
            InetAddress.getLoopbackAddress(), server.localPort, prepared(offer, bytes),
            TransferStartMode.NEW, ResumeStatus(ResumeState.NONE), TransferPauseController(), {}, {}
        )

        assertTrue(result.isSuccess)
        serverJob.await()
        server.close()
    }
    @Test
    fun `query writes v4 resume request reads bounded status and closes socket`() = runBlocking {
        val offer = offer(fileSize = TransferProtocol.CHUNK_SIZE.toLong())
        val expected = prefixStatus(ByteArray(TransferProtocol.CHUNK_SIZE) { 7 })
        val server = ServerSocket(0)
        val closed = CompletableDeferred<Unit>()
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                socket.soTimeout = 2_000
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertEquals(ConnectionKind.RESUME_QUERY, ConnectionProtocol.readPreamble(input))
                assertEquals(offer, ResumeProtocol.readOffer(input))
                ResumeProtocol.writeStatus(output, expected)
                output.flush()
                assertEquals(-1, input.read())
                closed.complete(Unit)
            }
        }

        val actual = FileTransferClient().queryResume(
            InetAddress.getLoopbackAddress(), server.localPort, offer
        )

        assertStatusEquals(expected, actual)
        withTimeout(3_000) { closed.await() }
        serverJob.await()
        server.close()
    }

    @Test
    fun `resume validates prefix on one stream starts at checkpoint and emits initial progress`() = runBlocking {
        val prefix = ByteArray(TransferProtocol.CHUNK_SIZE) { (it % 251).toByte() }
        val suffix = byteArrayOf(11, 12, 13)
        val bytes = prefix + suffix
        val offer = offer(fileSize = bytes.size.toLong())
        val expectedStatus = prefixStatus(prefix)
        var opens = 0
        val prepared = prepared(offer, bytes) { opens++ }
        val progress = mutableListOf<FileTransferProgress>()
        val server = ServerSocket(0)
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                socket.soTimeout = 3_000
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertStart(input, offer, TransferStartMode.RESUME, expectedStatus)
                writeReady(output)
                assertEquals(TransferFrameType.CHUNK, TransferFrameCodec.readType(input))
                val frame = ChunkCodec.read(input, expectedIndex = 1, expectedLength = suffix.size)
                assertArrayEquals(suffix, frame.data)
                output.writeByte(TransferProtocol.ACK)
                output.flush()
                assertEquals(TransferProtocol.COMPLETE, input.readUnsignedByte())
                assertArrayEquals(sha256(bytes), ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully))
                output.writeByte(TransferProtocol.SUCCESS)
                output.flush()
            }
        }

        val result = FileTransferClient().sendPrepared(
            InetAddress.getLoopbackAddress(),
            server.localPort,
            prepared,
            TransferStartMode.RESUME,
            expectedStatus,
            TransferPauseController(),
            {},
            progress::add
        )

        assertTrue(result.isSuccess)
        assertEquals(1, opens)
        assertEquals(
            listOf(
                FileTransferProgress(prefix.size.toLong(), bytes.size.toLong()),
                FileTransferProgress(bytes.size.toLong(), bytes.size.toLong())
            ),
            progress
        )
        serverJob.await()
        server.close()
    }

    @Test
    fun `resume prefix mismatch fails before any transfer start bytes`() = runBlocking {
        val expectedPrefix = ByteArray(TransferProtocol.CHUNK_SIZE) { 1 }
        val actualBytes = ByteArray(TransferProtocol.CHUNK_SIZE + 1) { 2 }
        val expectedStatus = prefixStatus(expectedPrefix)
        val offer = offer(fileSize = actualBytes.size.toLong())
        val server = ServerSocket(0).apply { soTimeout = 1_000 }
        val received = CompletableDeferred<ByteArray>()
        val serverJob = async(Dispatchers.IO) {
            val bytes = try {
                server.accept().use { socket ->
                    socket.soTimeout = 1_000
                    val sink = ByteArrayOutputStream()
                    val buffer = ByteArray(128)
                    while (true) {
                        val read = try {
                            socket.getInputStream().read(buffer)
                        } catch (_: SocketTimeoutException) {
                            break
                        }
                        if (read < 0) break
                        sink.write(buffer, 0, read)
                    }
                    sink.toByteArray()
                }
            } catch (_: SocketTimeoutException) {
                ByteArray(0)
            }
            received.complete(bytes)
        }

        val result = FileTransferClient().sendPrepared(
            InetAddress.getLoopbackAddress(),
            server.localPort,
            prepared(offer, actualBytes),
            TransferStartMode.RESUME,
            expectedStatus,
            TransferPauseController(),
            {},
            {}
        )

        assertTrue(result.isFailure)
        assertArrayEquals(ByteArray(0), withTimeout(3_000) { received.await() })
        serverJob.await()
        server.close()
    }

    @Test
    fun `new and restart transfers start from chunk zero`() = runBlocking {
        for (mode in listOf(TransferStartMode.NEW, TransferStartMode.RESTART)) {
            val bytes = byteArrayOf(3, 4, 5)
            val offer = offer(fileSize = bytes.size.toLong())
            val expectedStatus = if (mode == TransferStartMode.NEW) {
                ResumeStatus(ResumeState.NONE)
            } else {
                ResumeStatus(ResumeState.INVALID)
            }
            val server = ServerSocket(0)
            val serverJob = async(Dispatchers.IO) {
                server.accept().use { socket ->
                    socket.soTimeout = 3_000
                    val input = DataInputStream(socket.getInputStream())
                    val output = DataOutputStream(socket.getOutputStream())
                    assertStart(input, offer, mode, expectedStatus)
                    writeReady(output)
                    assertEquals(TransferFrameType.CHUNK, TransferFrameCodec.readType(input))
                    val frame = ChunkCodec.read(input, expectedIndex = 0, expectedLength = bytes.size)
                    assertArrayEquals(bytes, frame.data)
                    output.writeByte(TransferProtocol.ACK)
                    output.flush()
                    assertEquals(TransferProtocol.COMPLETE, input.readUnsignedByte())
                    assertArrayEquals(sha256(bytes), ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully))
                    output.writeByte(TransferProtocol.SUCCESS)
                    output.flush()
                }
            }

            val result = FileTransferClient().sendPrepared(
                InetAddress.getLoopbackAddress(),
                server.localPort,
                prepared(offer, bytes),
                mode,
                expectedStatus,
                TransferPauseController(),
                {},
                {}
            )

            assertTrue("$mode should succeed", result.isSuccess)
            serverJob.await()
            server.close()
        }
    }

    @Test
    fun `nack retries the identical chunk frame exactly three attempts`() = runBlocking {
        val bytes = byteArrayOf(8, 9)
        val offer = offer(fileSize = bytes.size.toLong())
        val expectedStatus = ResumeStatus(ResumeState.NONE)
        val server = ServerSocket(0)
        val frames = mutableListOf<ByteArray>()
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                socket.soTimeout = 3_000
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertStart(input, offer, TransferStartMode.NEW, expectedStatus)
                writeReady(output)
                repeat(3) {
                    assertEquals(TransferFrameType.CHUNK, TransferFrameCodec.readType(input))
                    val frame = ChunkCodec.read(input, expectedIndex = 0, expectedLength = bytes.size)
                    frames += encodeChunk(frame.index, frame.data, frame.digest)
                    output.writeByte(TransferProtocol.NACK)
                    output.flush()
                }
            }
        }

        val result = FileTransferClient().sendPrepared(
            InetAddress.getLoopbackAddress(),
            server.localPort,
            prepared(offer, bytes),
            TransferStartMode.NEW,
            expectedStatus,
            TransferPauseController(),
            {},
            {}
        )

        assertTrue(result.isFailure)
        serverJob.await()
        assertEquals(3, frames.size)
        assertArrayEquals(frames[0], frames[1])
        assertArrayEquals(frames[0], frames[2])
        server.close()
    }

    @Test
    fun `sender requires receiver success after complete digest`() = runBlocking {
        val bytes = byteArrayOf(1)
        val offer = offer(fileSize = 1)
        val expectedStatus = ResumeStatus(ResumeState.NONE)
        val server = ServerSocket(0)
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertStart(input, offer, TransferStartMode.NEW, expectedStatus)
                writeReady(output)
                assertEquals(TransferFrameType.CHUNK, TransferFrameCodec.readType(input))
                ChunkCodec.read(input, expectedIndex = 0, expectedLength = 1)
                output.writeByte(TransferProtocol.ACK)
                output.flush()
                assertEquals(TransferProtocol.COMPLETE, input.readUnsignedByte())
                assertArrayEquals(sha256(bytes), ByteArray(ChunkCodec.DIGEST_SIZE).also(input::readFully))
                output.writeByte(TransferProtocol.FAILURE)
                output.flush()
            }
        }

        val result = FileTransferClient().sendPrepared(
            InetAddress.getLoopbackAddress(), server.localPort, prepared(offer, bytes),
            TransferStartMode.NEW, expectedStatus, TransferPauseController(), {}, {}
        )

        assertTrue(result.isFailure)
        serverJob.await()
        server.close()
    }

    private fun prepared(
        offer: TransferOffer,
        bytes: ByteArray,
        onOpen: () -> Unit = {}
    ) = PreparedTransfer(
        offer = offer,
        source = SendFileSource(
            displayName = offer.fileName,
            mimeType = offer.mimeType,
            length = offer.fileSize,
            sourceUri = "content://test/${offer.transferId}",
            lastModified = 1L
        ) {
            onOpen()
            ByteArrayInputStream(bytes)
        },
        resumeStatus = ResumeStatus(ResumeState.NONE)
    )

    private fun offer(fileSize: Long) = TransferOffer(
        transferId = UUID.randomUUID().toString(),
        senderDeviceId = "sender-1",
        fileName = "file.bin",
        mimeType = "application/octet-stream",
        fileSize = fileSize
    )

    private fun prefixStatus(bytes: ByteArray): ResumeStatus {
        val prefix = PrefixDigestScanner.scan(
            ByteArrayInputStream(bytes), bytes.size.toLong(), TransferProtocol.CHUNK_SIZE
        )
        return ResumeStatus(
            state = ResumeState.AVAILABLE,
            confirmedBytes = prefix.scannedBytes,
            nextChunkIndex = prefix.nextChunkIndex,
            chainDigest = prefix.chainDigest,
            lastChunkHash = prefix.lastChunkHash
        )
    }

    private fun assertStart(
        input: DataInputStream,
        offer: TransferOffer,
        mode: TransferStartMode,
        expectedStatus: ResumeStatus
    ) {
        assertEquals(ConnectionKind.TRANSFER_START, ConnectionProtocol.readPreamble(input))
        assertEquals(offer, ResumeProtocol.readOffer(input))
        assertEquals(mode, ResumeProtocol.readStartMode(input))
        assertStatusEquals(expectedStatus, ResumeProtocol.readStatus(input, offer))
    }

    private fun assertStatusEquals(expected: ResumeStatus, actual: ResumeStatus) {
        assertEquals(expected.state, actual.state)
        assertEquals(expected.confirmedBytes, actual.confirmedBytes)
        assertEquals(expected.nextChunkIndex, actual.nextChunkIndex)
        assertArrayEquals(expected.chainDigest, actual.chainDigest)
        assertArrayEquals(expected.lastChunkHash, actual.lastChunkHash)
    }

    private fun writeReady(output: DataOutputStream) {
        ResumeProtocol.writeStartResponse(output, TransferStartResponse.READY)
        output.flush()
    }

    private fun encodeChunk(index: Int, data: ByteArray, digest: ByteArray): ByteArray =
        ByteArrayOutputStream().also { bytes ->
            DataOutputStream(bytes).use { output ->
                output.writeInt(index)
                output.writeInt(data.size)
                output.write(data)
                output.write(digest)
            }
        }.toByteArray()

    private fun sha256(bytes: ByteArray): ByteArray =
        MessageDigest.getInstance("SHA-256").digest(bytes)

    private fun assertThrowsSocketTimeout(block: () -> Unit) {
        try {
            block()
        } catch (_: SocketTimeoutException) {
            return
        }
        throw AssertionError("Expected SocketTimeoutException")
    }
}
