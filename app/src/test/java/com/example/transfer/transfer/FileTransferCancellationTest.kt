package com.example.transfer.transfer

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.ConnectionKind
import com.example.transfer.protocol.ConnectionProtocol
import com.example.transfer.protocol.ResumeProtocol
import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferFrameCodec
import com.example.transfer.protocol.TransferFrameType
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.resume.PreparedTransfer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.util.UUID

class FileTransferCancellationTest {
    @Test
    fun `cancel active closes a socket waiting for ack`() = runBlocking {
        val server = ServerSocket(0)
        val accepted = CompletableDeferred<Unit>()
        val serverJob = async(Dispatchers.IO) {
            server.accept().use {
                accepted.complete(Unit)
                it.getInputStream().read(ByteArray(128))
                Thread.sleep(5_000)
            }
        }
        val client = FileTransferClient()
        val send = async(Dispatchers.IO) {
            client.sendPrepared(
                InetAddress.getLoopbackAddress(), server.localPort,
                prepared(byteArrayOf(1)), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE),
                TransferPauseController(), {}, {}
            )
        }
        withTimeout(2_000) { accepted.await() }
        client.cancelActive()
        assertTrue(withTimeout(2_000) { send.await() }.isFailure)
        server.close()
        serverJob.cancel()
    }

    @Test
    fun `cancel active wakes a client paused after acknowledged chunk`() = runBlocking {
        val server = ServerSocket(0)
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE + 1) { (it % 251).toByte() }
        val pauseAcknowledged = CompletableDeferred<Unit>()
        val serverJob = async(Dispatchers.IO) {
            server.accept().use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = DataOutputStream(socket.getOutputStream())
                assertEquals(ConnectionKind.TRANSFER_START, ConnectionProtocol.readPreamble(input))
                ResumeProtocol.readOffer(input)
                assertEquals(TransferStartMode.NEW, ResumeProtocol.readStartMode(input))
                ResumeProtocol.readStatus(input)
                assertEquals(TransferFrameType.CHUNK, TransferFrameCodec.readType(input))
                ChunkCodec.read(input, expectedIndex = 0, expectedLength = TransferProtocol.CHUNK_SIZE)
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
                InetAddress.getLoopbackAddress(),
                server.localPort,
                prepared(bytes),
                TransferStartMode.NEW,
                ResumeStatus(ResumeState.NONE),
                controller,
                { state -> if (state == TransferPauseState.PAUSED) paused.complete(Unit) }
            ) { progress ->
                if (progress.confirmedBytes == TransferProtocol.CHUNK_SIZE.toLong()) {
                    controller.requestPause()
                }
            }
        }

        withTimeout(2_000) {
            pauseAcknowledged.await()
            paused.await()
        }
        client.cancelActive()

        assertTrue(withTimeout(2_000) { send.await() }.isFailure)
        assertEquals(TransferPauseState.CANCELLED, controller.state)
        server.close()
        serverJob.cancel()
    }

    private fun prepared(bytes: ByteArray): PreparedTransfer {
        val source = SendFileSource(
            "a.bin", "application/octet-stream", bytes.size.toLong(),
            sourceUri = "content://a", openStream = { ByteArrayInputStream(bytes) }
        )
        return PreparedTransfer(
            com.example.transfer.protocol.TransferOffer(
                UUID.randomUUID().toString(), "sender", source.displayName,
                source.mimeType, source.length
            ),
            source,
            ResumeStatus(ResumeState.NONE)
        )
    }
}
