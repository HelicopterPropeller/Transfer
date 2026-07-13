package com.example.transfer.transfer

import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.storage.IncomingFileStore
import com.example.transfer.storage.ReceivedFileHandle
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.net.ServerSocket
import java.net.Socket

class FileTransferServer(
    private val port: Int = DEFAULT_PORT,
    private val store: IncomingFileStore
) {
    @Volatile private var serverSocket: ServerSocket? = null
    @Volatile private var activeSocket: Socket? = null
    private var job: Job? = null

    fun start(
        scope: CoroutineScope,
        onStarted: (Int) -> Unit,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit,
        onError: (String) -> Unit
    ) {
        if (job?.isActive == true) return
        job = scope.launch(Dispatchers.IO) {
            try {
                ServerSocket(port).use { server ->
                    server.reuseAddress = true
                    serverSocket = server
                    onStarted(server.localPort)
                    while (isActive) {
                        val socket = server.accept()
                        activeSocket = socket
                        runCatching { receive(socket, onProgress, onComplete) }
                            .onFailure { if (isActive) onError(it.message ?: "接收文件失败") }
                        activeSocket = null
                    }
                }
            } catch (error: Exception) {
                if (isActive) onError(error.message ?: "接收服务启动失败")
            } finally {
                serverSocket = null
                activeSocket = null
            }
        }
    }

    fun stop() {
        job?.cancel()
        runCatching { activeSocket?.close() }
        runCatching { serverSocket?.close() }
        activeSocket = null
        serverSocket = null
        job = null
    }

    private suspend fun receive(
        socket: Socket,
        onProgress: (String, Int) -> Unit,
        onComplete: (String) -> Unit
    ) {
        socket.use {
            socket.soTimeout = SOCKET_TIMEOUT_MILLIS
            val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
            val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            var handle: ReceivedFileHandle? = null
            try {
                val header = TransferProtocol.readHeader(input)
                handle = store.create(header.fileName, header.mimeType)
                copyExactly(input, handle.output, header.fileSize) { progress ->
                    onProgress(handle.displayName, progress)
                }
                store.complete(handle)
                output.writeByte(TransferProtocol.SUCCESS)
                output.flush()
                onComplete(handle.displayName)
            } catch (error: Exception) {
                handle?.let { runCatching { store.abort(it) } }
                runCatching {
                    output.writeByte(TransferProtocol.FAILURE)
                    output.flush()
                }
                throw error
            }
        }
    }

    private fun copyExactly(
        input: java.io.InputStream,
        output: java.io.OutputStream,
        length: Long,
        onProgress: (Int) -> Unit
    ) {
        val buffer = ByteArray(BUFFER_SIZE)
        var received = 0L
        if (length == 0L) onProgress(100)
        while (received < length) {
            val wanted = minOf(buffer.size.toLong(), length - received).toInt()
            val count = input.read(buffer, 0, wanted)
            if (count < 0) throw EOFException("文件传输提前中断")
            output.write(buffer, 0, count)
            received += count
            onProgress(TransferProgress.percent(received, length))
        }
    }

    companion object {
        const val DEFAULT_PORT = 42043
        private const val SOCKET_TIMEOUT_MILLIS = 15_000
        private const val BUFFER_SIZE = 64 * 1024
    }
}
