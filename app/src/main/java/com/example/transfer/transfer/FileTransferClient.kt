package com.example.transfer.transfer

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.TransferHeader
import com.example.transfer.protocol.TransferProtocol
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.InputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket

class FileTransferClient {
    @Volatile private var activeSocket: Socket? = null

    suspend fun send(
        host: InetAddress,
        port: Int,
        source: SendFileSource,
        onProgress: (Int) -> Unit
    ): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val socket = Socket()
            activeSocket = socket
            try {
                socket.use {
                    socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS)
                    socket.soTimeout = SOCKET_TIMEOUT_MILLIS
                    val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                    val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                    TransferProtocol.writeHeader(output, TransferHeader(source.displayName, source.mimeType, source.length))
                    output.flush()
                    source.openStream().use { fileInput ->
                        sendChunks(fileInput, source.length, input, output, onProgress)
                    }
                    if (input.readUnsignedByte() != TransferProtocol.COMPLETE) {
                        error("Receiver could not finalize the file")
                    }
                    if (source.length == 0L) onProgress(100)
                }
            } finally {
                if (activeSocket === socket) activeSocket = null
            }
        }
    }

    fun cancelActive() {
        runCatching { activeSocket?.close() }
        activeSocket = null
    }

    private fun sendChunks(
        fileInput: InputStream,
        total: Long,
        input: DataInputStream,
        output: DataOutputStream,
        onProgress: (Int) -> Unit
    ) {
        var confirmed = 0L
        var index = 0
        while (confirmed < total) {
            val length = minOf(TransferProtocol.CHUNK_SIZE.toLong(), total - confirmed).toInt()
            val frame = ChunkCodec.create(index, readExactly(fileInput, length))
            var acknowledged = false
            for (attempt in 1..MAX_ATTEMPTS) {
                ChunkCodec.write(output, frame)
                output.flush()
                when (input.readUnsignedByte()) {
                    TransferProtocol.ACK -> { acknowledged = true; break }
                    TransferProtocol.NACK -> Unit
                    TransferProtocol.FATAL -> error("Receiver rejected the transfer")
                    else -> error("Unknown chunk response")
                }
            }
            if (!acknowledged) error("Chunk $index failed verification after $MAX_ATTEMPTS attempts")
            confirmed += length
            onProgress(TransferProgress.percent(confirmed, total))
            index++
        }
    }

    private fun readExactly(input: InputStream, length: Int): ByteArray {
        val data = ByteArray(length)
        var offset = 0
        while (offset < length) {
            val read = input.read(data, offset, length - offset)
            if (read < 0) throw EOFException("Source file ended early")
            offset += read
        }
        return data
    }

    companion object {
        private const val CONNECT_TIMEOUT_MILLIS = 5_000
        private const val SOCKET_TIMEOUT_MILLIS = 15_000
        private const val MAX_ATTEMPTS = 3
    }
}
