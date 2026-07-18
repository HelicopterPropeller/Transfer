package com.example.transfer.apkshare

import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.IOException
import java.io.OutputStream
import java.net.Inet4Address
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.ArrayDeque
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

interface ApkDownloadListener {
    fun onStarted(totalBytes: Long)
    fun onProgress(bytesSent: Long, totalBytes: Long)
    fun onCompleted()
    fun onFailed(message: String)
}

class LocalApkHttpServer(
    callbackExecutor: Executor = ForkJoinPool.commonPool(),
    private val serverSocketFactory: () -> ServerSocket = ::ServerSocket,
) : Closeable {
    private val lifecycleLock = Any()
    private val activeClients = ConcurrentHashMap.newKeySet<Socket>()
    private val callbacks = SerialCallbackDispatcher(callbackExecutor)
    private val clientNumber = AtomicInteger()
    private val clients = Executors.newFixedThreadPool(2) { runnable ->
        Thread(runnable, "apk-share-client-${clientNumber.incrementAndGet()}")
    }

    @Volatile
    private var serverSocket: ServerSocket? = null
    private var acceptThread: Thread? = null
    private var closed = false

    fun start(
        selectedAddress: SelectedLanAddress,
        artifact: ApkArtifact,
        session: ApkShareSession,
        listener: ApkDownloadListener,
    ): Int = synchronized(lifecycleLock) {
        check(serverSocket == null && !closed) { "Server has already been started or closed" }
        require(selectedAddress.prefixLength.toInt() in 0..32) { "Invalid IPv4 prefix length" }

        val socket = serverSocketFactory()
        var bound = false
        try {
            socket.reuseAddress = true
            socket.bind(InetSocketAddress(selectedAddress.address, 0), CLIENT_BACKLOG)
            bound = true
        } finally {
            if (!bound) closeQuietly(socket)
        }
        serverSocket = socket
        acceptThread = Thread(
            { acceptClients(socket, selectedAddress, artifact, session, listener) },
            "apk-share-accept",
        ).apply { start() }
        socket.localPort
    }

    private fun acceptClients(
        socket: ServerSocket,
        selectedAddress: SelectedLanAddress,
        artifact: ApkArtifact,
        session: ApkShareSession,
        listener: ApkDownloadListener,
    ) {
        while (!socket.isClosed) {
            val client = try {
                socket.accept()
            } catch (_: SocketException) {
                break
            } catch (_: IOException) {
                if (socket.isClosed) break else continue
            }

            activeClients += client
            try {
                clients.execute {
                    try {
                        client.use {
                            serveBoundedRequest(it, selectedAddress, artifact, session, listener)
                        }
                    } finally {
                        activeClients -= client
                    }
                }
            } catch (_: RejectedExecutionException) {
                activeClients -= client
                closeQuietly(client)
            }
        }
    }

    private fun serveBoundedRequest(
        socket: Socket,
        selectedAddress: SelectedLanAddress,
        artifact: ApkArtifact,
        session: ApkShareSession,
        listener: ApkDownloadListener,
    ) {
        socket.soTimeout = CLIENT_TIMEOUT_MILLIS
        val output = socket.getOutputStream()

        if (!isInSelectedSubnet(socket.inetAddress as? Inet4Address, selectedAddress)) {
            writeError(output, "403 Forbidden")
            return
        }

        when (val parsed = readRequest(socket)) {
            RequestReadResult.HeaderTooLarge -> writeError(output, "431 Request Header Fields Too Large")
            RequestReadResult.Malformed -> writeError(output, "400 Bad Request")
            is RequestReadResult.Valid -> serveParsedRequest(
                request = parsed.request,
                output = output,
                artifact = artifact,
                session = session,
                listener = listener,
            )
        }
    }

    private fun serveParsedRequest(
        request: HttpRequest,
        output: OutputStream,
        artifact: ApkArtifact,
        session: ApkShareSession,
        listener: ApkDownloadListener,
    ) {
        val trackedOutput = DownloadTrackingOutputStream(
            delegate = output,
            tracksDownload = DOWNLOAD_TARGET.matches(request.target),
            totalBytes = artifact.size,
            listener = listener,
            callbacks = callbacks,
        )
        try {
            ApkHttpProtocol(artifact, session).respond(request, trackedOutput)
            trackedOutput.reportCompletion()
        } catch (exception: IOException) {
            trackedOutput.reportFailure(exception)
        } catch (exception: RuntimeException) {
            trackedOutput.reportFailure(exception)
        }
    }

    private fun readRequest(socket: Socket): RequestReadResult {
        val bytes = ByteArrayOutputStream(MAX_HEADER_BYTES)
        var matchedTerminatorBytes = 0
        try {
            while (bytes.size() < MAX_HEADER_BYTES) {
                val value = socket.getInputStream().read()
                if (value < 0) return RequestReadResult.Malformed
                bytes.write(value)

                matchedTerminatorBytes = when {
                    matchedTerminatorBytes == 0 && value == CR -> 1
                    matchedTerminatorBytes == 1 && value == LF -> 2
                    matchedTerminatorBytes == 2 && value == CR -> 3
                    matchedTerminatorBytes == 3 && value == LF -> 4
                    value == CR -> 1
                    else -> 0
                }
                if (matchedTerminatorBytes == 4) {
                    return parseRequest(bytes.toByteArray())
                }
            }
        } catch (_: IOException) {
            return RequestReadResult.Malformed
        }
        return RequestReadResult.HeaderTooLarge
    }

    private fun parseRequest(bytes: ByteArray): RequestReadResult {
        val text = String(bytes, UTF_8)
        if (!text.endsWith("\r\n\r\n")) return RequestReadResult.Malformed
        val lines = text.dropLast(4).split("\r\n")
        val requestParts = lines.firstOrNull()?.split(' ') ?: return RequestReadResult.Malformed
        if (requestParts.size != 3 || requestParts.any(String::isEmpty)) {
            return RequestReadResult.Malformed
        }
        val method = requestParts[0]
        val target = requestParts[1]
        val version = requestParts[2]
        if (!HTTP_TOKEN.matches(method) || target.any { it <= ' ' || it.code == 0x7f }) {
            return RequestReadResult.Malformed
        }

        val headers = linkedMapOf<String, String>()
        for (line in lines.drop(1)) {
            val separator = line.indexOf(':')
            if (separator <= 0) return RequestReadResult.Malformed
            val name = line.substring(0, separator)
            val rawValue = line.substring(separator + 1)
            if (!HTTP_TOKEN.matches(name) || rawValue.any(::isForbiddenHeaderValueCharacter)) {
                return RequestReadResult.Malformed
            }
            val value = rawValue.trim(' ', '\t')
            headers[name] = value
        }
        return RequestReadResult.Valid(HttpRequest(method, target, version, headers))
    }

    private fun isInSelectedSubnet(
        clientAddress: Inet4Address?,
        selectedAddress: SelectedLanAddress,
    ): Boolean {
        clientAddress ?: return false
        var bitsRemaining = selectedAddress.prefixLength.toInt()
        val clientBytes = clientAddress.address
        val selectedBytes = selectedAddress.address.address
        for (index in clientBytes.indices) {
            if (bitsRemaining <= 0) return true
            val bitsInByte = minOf(bitsRemaining, 8)
            val mask = (0xff shl (8 - bitsInByte)) and 0xff
            if ((clientBytes[index].toInt() and mask) != (selectedBytes[index].toInt() and mask)) {
                return false
            }
            bitsRemaining -= bitsInByte
        }
        return true
    }

    override fun close() {
        val socket: ServerSocket?
        val thread: Thread?
        synchronized(lifecycleLock) {
            if (closed) return
            closed = true
            socket = serverSocket
            thread = acceptThread
            serverSocket = null
            acceptThread = null
        }

        callbacks.close()
        closeQuietly(socket)
        activeClients.forEach(::closeQuietly)
        clients.shutdownNow()
        joinBounded(thread)
        awaitClientsBounded()
        activeClients.forEach(::closeQuietly)
    }

    private fun joinBounded(thread: Thread?) {
        if (thread == null || thread === Thread.currentThread()) return
        try {
            thread.join(SHUTDOWN_TIMEOUT_MILLIS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun awaitClientsBounded() {
        try {
            clients.awaitTermination(SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun closeQuietly(closeable: Closeable?) {
        try {
            closeable?.close()
        } catch (_: IOException) {
            // Closing is best-effort and deliberately idempotent.
        }
    }

    private fun writeError(output: OutputStream, status: String) {
        val body = "$status\n".toByteArray(UTF_8)
        val headers = buildString {
            append("HTTP/1.1 $status\r\n")
            append("Content-Type: text/plain; charset=utf-8\r\n")
            append("Content-Length: ${body.size}\r\n")
            append("Connection: close\r\n")
            append("Cache-Control: no-store\r\n")
            append("X-Content-Type-Options: nosniff\r\n")
            append("\r\n")
        }.toByteArray(UTF_8)
        try {
            output.write(headers)
            output.write(body)
            output.flush()
        } catch (_: IOException) {
            // The peer may disconnect while an error is being returned.
        }
    }

    private sealed interface RequestReadResult {
        data class Valid(val request: HttpRequest) : RequestReadResult
        data object HeaderTooLarge : RequestReadResult
        data object Malformed : RequestReadResult
    }

    private class DownloadTrackingOutputStream(
        private val delegate: OutputStream,
        private val tracksDownload: Boolean,
        private val totalBytes: Long,
        private val listener: ApkDownloadListener,
        private val callbacks: SerialCallbackDispatcher,
    ) : OutputStream() {
        private var responseStarted = false
        private var downloadStarted = false
        private var bodyBytesWritten = 0L
        private var terminalCallbackReported = false

        override fun write(value: Int) {
            val byte = byteArrayOf(value.toByte())
            write(byte, 0, 1)
        }

        override fun write(bytes: ByteArray, offset: Int, length: Int) {
            if (!responseStarted) {
                responseStarted = true
                downloadStarted = tracksDownload && hasSuccessfulStatus(bytes, offset, length)
                if (downloadStarted) notifyListener { onStarted(totalBytes) }
                delegate.write(bytes, offset, length)
                return
            }

            delegate.write(bytes, offset, length)
            if (downloadStarted && length > 0) {
                bodyBytesWritten += length
                notifyListener { onProgress(bodyBytesWritten, totalBytes) }
            }
        }

        override fun flush() = delegate.flush()

        fun reportCompletion() {
            if (
                downloadStarted &&
                !terminalCallbackReported &&
                bodyBytesWritten == totalBytes
            ) {
                terminalCallbackReported = true
                notifyListener { onCompleted() }
            }
        }

        fun reportFailure(exception: Exception) {
            if (downloadStarted && !terminalCallbackReported) {
                terminalCallbackReported = true
                val message = exception.message ?: "APK download failed"
                notifyListener { onFailed(message) }
            }
        }

        private fun hasSuccessfulStatus(bytes: ByteArray, offset: Int, length: Int): Boolean {
            if (length < SUCCESS_STATUS.size) return false
            for (index in SUCCESS_STATUS.indices) {
                if (bytes[offset + index] != SUCCESS_STATUS[index]) return false
            }
            return true
        }

        private fun notifyListener(callback: ApkDownloadListener.() -> Unit) {
            callbacks.dispatch { listener.callback() }
        }
    }

    private class SerialCallbackDispatcher(private val executor: Executor) : Closeable {
        private val lock = Any()
        private val pending = ArrayDeque<() -> Unit>()
        private var drainScheduled = false
        private var closed = false

        fun dispatch(callback: () -> Unit) {
            val scheduleDrain = synchronized(lock) {
                if (closed) return
                pending.addLast(callback)
                if (drainScheduled) {
                    false
                } else {
                    drainScheduled = true
                    true
                }
            }
            if (!scheduleDrain) return

            try {
                executor.execute(::drain)
            } catch (_: RejectedExecutionException) {
                synchronized(lock) {
                    pending.clear()
                    drainScheduled = false
                }
            }
        }

        private fun drain() {
            while (true) {
                val callback = synchronized(lock) {
                    if (closed) {
                        pending.clear()
                        drainScheduled = false
                        null
                    } else {
                        pending.pollFirst().also { next ->
                            if (next == null) drainScheduled = false
                        }
                    }
                } ?: return

                runCatching(callback)
            }
        }

        override fun close() {
            synchronized(lock) {
                if (closed) return
                closed = true
                pending.clear()
            }
        }
    }

    private companion object {
        fun isForbiddenHeaderValueCharacter(character: Char): Boolean =
            character.code == 0x7f || character.code < 0x20 && character != '\t'

        const val CLIENT_BACKLOG = 4
        const val CLIENT_TIMEOUT_MILLIS = 10_000
        const val MAX_HEADER_BYTES = 8 * 1024
        const val SHUTDOWN_TIMEOUT_MILLIS = 1_000L
        const val CR = 13
        const val LF = 10
        val HTTP_TOKEN = Regex("[!#$%&'*+.^_`|~0-9A-Za-z-]+")
        val DOWNLOAD_TARGET = Regex("^/i/[a-f0-9]{48}/app\\.apk$")
        val SUCCESS_STATUS = "HTTP/1.1 200 OK\r\n".toByteArray(UTF_8)
    }
}
