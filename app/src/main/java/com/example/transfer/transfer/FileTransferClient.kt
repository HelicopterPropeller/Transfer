package com.example.transfer.transfer

import com.example.transfer.protocol.TransferHeader
import com.example.transfer.protocol.TransferProtocol
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket

class FileTransferClient {
    suspend fun send(
        host: InetAddress,
        port: Int,
        source: SendFileSource,
        onProgress: (Int) -> Unit
    ): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            Socket().use { socket ->
                socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS)
                socket.soTimeout = SOCKET_TIMEOUT_MILLIS
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                TransferProtocol.writeHeader(
                    output,
                    TransferHeader(source.displayName, source.mimeType, source.length)
                )
                source.openStream().use { fileInput ->
                    copyExactly(fileInput, output, source.length, onProgress)
                }
                output.flush()
                if (input.readUnsignedByte() != TransferProtocol.SUCCESS) {
                    error("接收端保存文件失败")
                }
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
        var transferred = 0L
        if (length == 0L) onProgress(100)
        while (transferred < length) {
            val wanted = minOf(buffer.size.toLong(), length - transferred).toInt()
            val read = input.read(buffer, 0, wanted)
            if (read < 0) throw EOFException("源文件读取提前结束")
            output.write(buffer, 0, read)
            transferred += read
            onProgress(TransferProgress.percent(transferred, length))
        }
    }

    companion object {
        private const val CONNECT_TIMEOUT_MILLIS = 5_000
        private const val SOCKET_TIMEOUT_MILLIS = 15_000
        private const val BUFFER_SIZE = 64 * 1024
    }
}
