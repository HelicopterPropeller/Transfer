package com.example.transfer.transfer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.net.InetAddress
import java.net.ServerSocket

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
            client.send(
                InetAddress.getLoopbackAddress(), server.localPort,
                SendFileSource("a.bin", "application/octet-stream", 1) { ByteArrayInputStream(byteArrayOf(1)) }
            ) {}
        }
        withTimeout(2_000) { accepted.await() }
        client.cancelActive()
        assertTrue(withTimeout(2_000) { send.await() }.isFailure)
        server.close()
        serverJob.cancel()
    }
}
