package com.example.transfer.apkshare

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.BindException
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class LocalApkHttpServerTest {
    @get:Rule
    val temporary = TemporaryFolder()

    @Test
    fun `socket server serves landing page then closes cleanly`() {
        LocalApkHttpServer().use { server ->
            val session = fixedSession()
            val port = server.start(loopbackSelection(), artifactContaining("apk"), session, NoOpListener)

            val response = request(port, "GET /i/${session.token}/ HTTP/1.1\r\nHost: local\r\n\r\n")

            assertEquals("HTTP/1.1 200 OK", response.statusLine)
            assertTrue(response.body.toString(UTF_8).contains("/i/${session.token}/app.apk"))
            assertTrue(session.authorize(session.token))
        }
    }

    @Test
    fun `socket download streams the complete artifact and reports listener lifecycle`() {
        val body = "complete-apk-over-socket".toByteArray(UTF_8)
        val session = fixedSession()
        val listener = RecordingListener()
        LocalApkHttpServer().use { server ->
            val port = server.start(loopbackSelection(), artifactContaining(body), session, listener)

            val response = request(
                port,
                "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n",
            )

            assertEquals("HTTP/1.1 200 OK", response.statusLine)
            assertArrayEquals(body, response.body)
            assertTrue(listener.completed.await(2, TimeUnit.SECONDS))
            assertEquals(listOf(body.size.toLong()), listener.startedTotals)
            assertEquals(body.size.toLong(), listener.progress.last().first)
            assertEquals(body.size.toLong(), listener.progress.last().second)
            assertTrue(listener.failures.isEmpty())
            assertFalse(session.authorize(session.token))
        }
    }

    @Test
    fun `range header is rejected and leaves token retryable`() {
        val session = fixedSession()
        LocalApkHttpServer().use { server ->
            val port = server.start(loopbackSelection(), artifactContaining("secret"), session, NoOpListener)

            val response = request(
                port,
                "GET /i/${session.token}/app.apk HTTP/1.1\r\n" +
                    "Host: local\r\nRange: bytes=0-1\r\n\r\n",
            )

            assertEquals("HTTP/1.1 416 Range Not Satisfiable", response.statusLine)
            assertFalse(response.body.toString(UTF_8).contains("secret"))
            assertTrue(session.authorize(session.token))
        }
    }

    @Test
    fun `bare line breaks and forbidden controls in header values are malformed`() {
        val malformedHeaders = listOf(
            "X-Hidden: ok\nRange: bytes=0-1" to "bare LF concealed Range",
            "X-Bad: before\rafter" to "bare CR",
            "X-Bad: before\u0001after" to "CTL",
            "X-Bad: before\u007fafter" to "DEL",
        )

        malformedHeaders.forEach { (header, description) ->
            val session = fixedSession()
            LocalApkHttpServer().use { server ->
                val port = server.start(
                    loopbackSelection(),
                    artifactContaining("secret"),
                    session,
                    NoOpListener,
                )
                val response = request(
                    port,
                    "GET /i/${session.token}/app.apk HTTP/1.1\r\n" +
                        "Host: local\r\n$header\r\n\r\n",
                )

                assertEquals(description, "HTTP/1.1 400 Bad Request", response.statusLine)
                assertFalse(description, response.body.toString(UTF_8).contains("secret"))
                assertTrue(description, session.authorize(session.token))
            }
        }
    }

    @Test
    fun `more than eight kibibytes of headers is rejected`() {
        val session = fixedSession()
        LocalApkHttpServer().use { server ->
            val port = server.start(loopbackSelection(), artifactContaining("secret"), session, NoOpListener)
            val oversized = "GET /i/${session.token}/app.apk HTTP/1.1\r\n" +
                "Host: local\r\nX-Fill: ${"a".repeat(8 * 1024)}\r\n\r\n"

            val response = request(port, oversized)

            assertEquals("HTTP/1.1 431 Request Header Fields Too Large", response.statusLine)
            assertFalse(response.body.toString(UTF_8).contains("secret"))
            assertTrue(session.authorize(session.token))
        }
    }

    @Test
    fun `non GET and malformed socket requests are rejected`() {
        val session = fixedSession()
        LocalApkHttpServer().use { server ->
            val port = server.start(loopbackSelection(), artifactContaining("secret"), session, NoOpListener)

            val methodResponse = request(
                port,
                "POST /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n",
            )
            val pathResponse = request(
                port,
                "GET /i/${session.token}/app.apk/extra HTTP/1.1\r\nHost: local\r\n\r\n",
            )

            assertEquals("HTTP/1.1 405 Method Not Allowed", methodResponse.statusLine)
            assertEquals("HTTP/1.1 404 Not Found", pathResponse.statusLine)
            assertFalse(methodResponse.body.toString(UTF_8).contains("secret"))
            assertFalse(pathResponse.body.toString(UTF_8).contains("secret"))
            assertTrue(session.authorize(session.token))
        }
    }

    @Test
    fun `a client in the selected subnet is allowed`() {
        val session = fixedSession()
        val serverAddress = ipv4("127.0.0.1")
        val clientAddress = ipv4("127.0.0.2")
        LocalApkHttpServer().use { server ->
            val port = server.start(
                SelectedLanAddress(serverAddress, 8),
                artifactContaining("same-subnet"),
                session,
                NoOpListener,
            )

            val response = request(
                port = port,
                rawRequest = "GET /i/${session.token}/ HTTP/1.1\r\nHost: local\r\n\r\n",
                localAddress = clientAddress,
                serverAddress = serverAddress,
            )

            assertEquals("HTTP/1.1 200 OK", response.statusLine)
        }
    }

    @Test
    fun `a client outside the selected subnet is forbidden`() {
        val session = fixedSession()
        val serverAddress = ipv4("127.0.0.1")
        val clientAddress = ipv4("127.0.0.2")
        LocalApkHttpServer().use { server ->
            val port = server.start(
                SelectedLanAddress(serverAddress, 32),
                artifactContaining("secret"),
                session,
                NoOpListener,
            )

            val response = request(
                port = port,
                rawRequest = "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n",
                localAddress = clientAddress,
                serverAddress = serverAddress,
            )

            assertEquals("HTTP/1.1 403 Forbidden", response.statusLine)
            assertFalse(response.body.toString(UTF_8).contains("secret"))
            assertTrue(session.authorize(session.token))
        }
    }

    @Test
    fun `second concurrent download attempt is rejected`() {
        val session = fixedSession()
        val listener = BlockingStartedListener()
        LocalApkHttpServer().use { server ->
            val body = ByteArray(8 * 1024 * 1024) { index -> (index % 251).toByte() }
            val port = server.start(loopbackSelection(), artifactContaining(body), session, listener)
            val first = connectedSocket(port).apply { receiveBufferSize = 1_024 }
            try {
                first.getOutputStream().write(
                    "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n".toByteArray(UTF_8),
                )
                first.getOutputStream().flush()
                assertTrue("First attempt did not start", listener.started.await(2, TimeUnit.SECONDS))

                val second = request(
                    port,
                    "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n",
                )

                assertEquals("HTTP/1.1 409 Conflict", second.statusLine)
                assertFalse(second.body.contentEquals(body))
            } finally {
                listener.release.countDown()
                first.readHttpResponse()
                first.close()
            }
        }
    }

    @Test
    fun `client interruption fails the attempt and permits a complete retry`() {
        val body = ByteArray(8 * 1024 * 1024) { index -> (index % 251).toByte() }
        val session = fixedSession()
        val listener = RecordingListener()
        LocalApkHttpServer().use { server ->
            val port = server.start(loopbackSelection(), artifactContaining(body), session, listener)
            val interrupted = connectedSocket(port).apply {
                receiveBufferSize = 1_024
                setSoLinger(true, 0)
            }
            interrupted.getOutputStream().write(
                "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n".toByteArray(UTF_8),
            )
            interrupted.getOutputStream().flush()
            assertTrue("Interrupted attempt did not start", listener.started.await(2, TimeUnit.SECONDS))

            interrupted.close()

            assertTrue("Interrupted attempt was not reported", listener.failed.await(3, TimeUnit.SECONDS))
            assertTrue(session.authorize(session.token))
            val retry = request(
                port,
                "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n",
                timeoutMillis = 10_000,
            )
            assertEquals("HTTP/1.1 200 OK", retry.statusLine)
            assertArrayEquals(body, retry.body)
            assertTrue(listener.completed.await(2, TimeUnit.SECONDS))
            assertFalse(session.authorize(session.token))
        }
    }

    @Test
    fun `successful download causes later socket attempt to be forbidden`() {
        val session = fixedSession()
        LocalApkHttpServer().use { server ->
            val port = server.start(loopbackSelection(), artifactContaining("one-shot"), session, NoOpListener)
            val request = "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n"

            assertEquals("HTTP/1.1 200 OK", request(port, request).statusLine)
            val duplicate = request(port, request)

            assertEquals("HTTP/1.1 403 Forbidden", duplicate.statusLine)
            assertFalse(duplicate.body.toString(UTF_8).contains("one-shot"))
        }
    }

    @Test
    fun `close terminates accept and client threads even with a partial request`() {
        val server = LocalApkHttpServer()
        val session = fixedSession()
        val port = server.start(loopbackSelection(), artifactContaining("apk"), session, NoOpListener)
        val stalled = connectedSocket(port)
        stalled.getOutputStream().write(
            "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost:".toByteArray(UTF_8),
        )
        stalled.getOutputStream().flush()
        assertEventually("Server did not start both accept and client workers") {
            apkServerThreads().size >= 2
        }

        server.close()

        assertEventually("LocalApkHttpServer leaked a live thread after close") {
            apkServerThreads().isEmpty()
        }
        stalled.soTimeout = 2_000
        val closedByServer = try {
            stalled.getInputStream().read() == -1
        } catch (_: IOException) {
            true
        } finally {
            stalled.close()
        }
        assertTrue("close() did not close an active client socket", closedByServer)
    }

    @Test
    fun `close preserves queued callbacks without waiting for a blocking caller callback`() {
        val callbackExecutor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "test-apk-blocking-callback")
        }
        val listener = UninterruptiblyBlockingListener()
        val server = LocalApkHttpServer(callbackExecutor = callbackExecutor)
        val session = fixedSession()
        val socket = try {
            val port = server.start(loopbackSelection(), artifactContaining("apk"), session, listener)
            connectedSocket(port).also { client ->
                client.getOutputStream().write(
                    "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n".toByteArray(UTF_8),
                )
                client.getOutputStream().flush()
            }
        } catch (exception: Exception) {
            listener.release.countDown()
            callbackExecutor.shutdownNow()
            throw exception
        }

        try {
            assertTrue("Started callback did not block", listener.started.await(2, TimeUnit.SECONDS))
            assertEquals("test-apk-blocking-callback", listener.callbackThreadName)
            assertEventually("Download did not finish while callback was blocked") {
                !session.authorize(session.token)
            }

            val closeMillis = measureTimeMillis { server.close() }

            assertTrue("close() waited on caller callback for ${closeMillis}ms", closeMillis < 2_000L)
            assertTrue("LocalApkHttpServer leaked an owned thread", apkServerThreads().isEmpty())
            assertFalse("Server closed its caller-owned executor", callbackExecutor.isShutdown)
            listener.release.countDown()
            callbackExecutor.submit {}.get(2, TimeUnit.SECONDS)
            assertEventually("Queued completion callbacks were lost during close") {
                listener.events.lastOrNull() == "completed"
            }
            assertEquals("started", listener.events.first())
            assertTrue(listener.events.contains("progress"))
            assertEquals("completed", listener.events.last())
        } finally {
            listener.release.countDown()
            socket.close()
            server.close()
            callbackExecutor.shutdownNow()
            callbackExecutor.awaitTermination(2, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `direct callback executor never runs a blocking listener on server client threads`() {
        val listener = UninterruptiblyBlockingListener()
        val server = LocalApkHttpServer(callbackExecutor = Executor { command -> command.run() })
        val session = fixedSession()
        val socket = try {
            val port = server.start(loopbackSelection(), artifactContaining("apk"), session, listener)
            connectedSocket(port).also { client ->
                client.getOutputStream().write(
                    "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n".toByteArray(UTF_8),
                )
                client.getOutputStream().flush()
            }
        } catch (exception: Exception) {
            listener.release.countDown()
            throw exception
        }

        try {
            assertTrue("Started callback did not block", listener.started.await(2, TimeUnit.SECONDS))
            assertFalse(
                "Listener ran on a server-owned client thread: ${listener.callbackThreadName}",
                listener.callbackThreadName.orEmpty().startsWith("apk-share-client-"),
            )

            server.close()

            assertEventually("Direct executor listener pinned a server-owned thread") {
                apkServerThreads().isEmpty()
            }
        } finally {
            listener.release.countDown()
            socket.close()
            server.close()
        }
    }

    @Test
    fun `close before caller executor drain preserves accepted terminal callbacks`() {
        val callbackExecutor = ControllableExecutor()
        val listener = EventRecordingListener()
        val server = LocalApkHttpServer(callbackExecutor = callbackExecutor)
        val session = fixedSession()
        val body = "apk".toByteArray(UTF_8)

        try {
            val port = server.start(loopbackSelection(), artifactContaining(body), session, listener)
            val response = request(
                port,
                "GET /i/${session.token}/app.apk HTTP/1.1\r\nHost: local\r\n\r\n",
            )
            assertEquals("HTTP/1.1 200 OK", response.statusLine)
            assertFalse(session.authorize(session.token))
            assertEventually("Callback drain was not handed to the caller executor") {
                callbackExecutor.hasPendingTasks()
            }

            server.close()
            callbackExecutor.runAll()

            assertEquals(listOf("started", "progress:${body.size}", "completed"), listener.events)
        } finally {
            server.close()
            callbackExecutor.runAll()
        }
    }

    @Test
    fun `bind failure closes the new socket and server remains safely closable`() {
        val failedSocket = FailingBindServerSocket()
        val server = LocalApkHttpServer(
            callbackExecutor = Executor { command -> command.run() },
            serverSocketFactory = { failedSocket },
        )

        try {
            server.start(loopbackSelection(), artifactContaining("apk"), fixedSession(), NoOpListener)
            fail("Expected bind failure")
        } catch (_: BindException) {
            // Expected from the injected socket.
        } finally {
            server.close()
        }

        assertTrue("Failed bind socket was not closed", failedSocket.isClosed)
        assertTrue("Failed start leaked a server-owned thread", apkServerThreads().isEmpty())
    }

    private fun artifactContaining(content: String): ApkArtifact =
        artifactContaining(content.toByteArray(UTF_8))

    private fun artifactContaining(content: ByteArray): ApkArtifact {
        val file = temporary.newFile().apply { writeBytes(content) }
        return ApkArtifact(
            file = file,
            fileName = "Transfer-test.apk",
            versionName = "test",
            versionCode = 1,
            size = file.length(),
            sha256 = "0".repeat(64),
        )
    }

    private fun fixedSession() = ApkShareSession.create(
        nowMillis = { 1_000L },
        randomBytes = { size -> ByteArray(size) { 7 } },
    )

    private fun loopbackSelection(prefixLength: Short = 8): SelectedLanAddress =
        SelectedLanAddress(ipv4("127.0.0.1"), prefixLength)

    private fun connectedSocket(
        port: Int,
        localAddress: InetAddress? = null,
        serverAddress: InetAddress = ipv4("127.0.0.1"),
        timeoutMillis: Int = 3_000,
    ): Socket = Socket().apply {
        if (localAddress != null) bind(InetSocketAddress(localAddress, 0))
        connect(InetSocketAddress(serverAddress, port), timeoutMillis)
        soTimeout = timeoutMillis
    }

    private fun request(
        port: Int,
        rawRequest: String,
        localAddress: InetAddress? = null,
        serverAddress: InetAddress = ipv4("127.0.0.1"),
        timeoutMillis: Int = 3_000,
    ): SocketHttpResponse = connectedSocket(
        port = port,
        localAddress = localAddress,
        serverAddress = serverAddress,
        timeoutMillis = timeoutMillis,
    ).use { socket ->
        runCatching {
            socket.getOutputStream().write(rawRequest.toByteArray(UTF_8))
            socket.getOutputStream().flush()
        }
        socket.readHttpResponse()
    }

    private fun Socket.readHttpResponse(): SocketHttpResponse {
        val bytes = ByteArrayOutputStream()
        val buffer = ByteArray(8 * 1024)
        while (true) {
            val read = getInputStream().read(buffer)
            if (read < 0) break
            bytes.write(buffer, 0, read)
        }
        return bytes.toByteArray().socketHttpResponse()
    }
}

private object NoOpListener : ApkDownloadListener {
    override fun onStarted(totalBytes: Long) = Unit
    override fun onProgress(bytesSent: Long, totalBytes: Long) = Unit
    override fun onCompleted() = Unit
    override fun onFailed(message: String) = Unit
}

private class RecordingListener : ApkDownloadListener {
    val started = CountDownLatch(1)
    val completed = CountDownLatch(1)
    val failed = CountDownLatch(1)
    val startedTotals = CopyOnWriteArrayList<Long>()
    val progress = CopyOnWriteArrayList<Pair<Long, Long>>()
    val failures = CopyOnWriteArrayList<String>()

    override fun onStarted(totalBytes: Long) {
        startedTotals += totalBytes
        started.countDown()
    }

    override fun onProgress(bytesSent: Long, totalBytes: Long) {
        progress += bytesSent to totalBytes
    }

    override fun onCompleted() {
        completed.countDown()
    }

    override fun onFailed(message: String) {
        failures += message
        failed.countDown()
    }
}

private class BlockingStartedListener : ApkDownloadListener {
    val started = CountDownLatch(1)
    val release = CountDownLatch(1)
    private val blockFirst = AtomicBoolean(true)

    override fun onStarted(totalBytes: Long) {
        if (blockFirst.compareAndSet(true, false)) {
            started.countDown()
            release.await(3, TimeUnit.SECONDS)
        }
    }

    override fun onProgress(bytesSent: Long, totalBytes: Long) = Unit
    override fun onCompleted() = Unit
    override fun onFailed(message: String) = Unit
}

private class UninterruptiblyBlockingListener : ApkDownloadListener {
    val started = CountDownLatch(1)
    val release = CountDownLatch(1)
    val events = CopyOnWriteArrayList<String>()
    var callbackThreadName: String? = null

    override fun onStarted(totalBytes: Long) {
        callbackThreadName = Thread.currentThread().name
        events += "started"
        started.countDown()
        var interrupted = false
        while (true) {
            try {
                release.await()
                break
            } catch (_: InterruptedException) {
                interrupted = true
            }
        }
        if (interrupted) Thread.currentThread().interrupt()
    }

    override fun onProgress(bytesSent: Long, totalBytes: Long) {
        events += "progress"
    }

    override fun onCompleted() {
        events += "completed"
    }

    override fun onFailed(message: String) {
        events += "failed"
    }
}

private class EventRecordingListener : ApkDownloadListener {
    val events = CopyOnWriteArrayList<String>()

    override fun onStarted(totalBytes: Long) {
        events += "started"
    }

    override fun onProgress(bytesSent: Long, totalBytes: Long) {
        events += "progress:$bytesSent"
    }

    override fun onCompleted() {
        events += "completed"
    }

    override fun onFailed(message: String) {
        events += "failed"
    }
}

private class ControllableExecutor : Executor {
    private val tasks = CopyOnWriteArrayList<Runnable>()

    override fun execute(command: Runnable) {
        tasks += command
    }

    fun hasPendingTasks(): Boolean = tasks.isNotEmpty()

    fun runAll() {
        while (true) {
            val task = tasks.firstOrNull() ?: return
            tasks.remove(task)
            task.run()
        }
    }
}

private class FailingBindServerSocket : ServerSocket() {
    override fun bind(endpoint: SocketAddress?, backlog: Int) {
        throw BindException("forced bind failure")
    }
}

private data class SocketHttpResponse(
    val statusLine: String,
    val headers: Map<String, String>,
    val body: ByteArray,
)

private fun ByteArray.socketHttpResponse(): SocketHttpResponse {
    val headerEnd = indexOfSocketHeaderEnd()
    val lines = String(this, 0, headerEnd - 4, UTF_8).split("\r\n")
    return SocketHttpResponse(
        statusLine = lines.first(),
        headers = lines.drop(1).associate { line ->
            val separator = line.indexOf(':')
            check(separator > 0) { "Malformed response header: $line" }
            line.substring(0, separator) to line.substring(separator + 1).trim()
        },
        body = copyOfRange(headerEnd, size),
    )
}

private fun ByteArray.indexOfSocketHeaderEnd(): Int {
    for (index in 0..size - 4) {
        if (
            this[index] == 13.toByte() &&
            this[index + 1] == 10.toByte() &&
            this[index + 2] == 13.toByte() &&
            this[index + 3] == 10.toByte()
        ) {
            return index + 4
        }
    }
    error("Response did not contain a complete HTTP header block")
}

private fun ipv4(value: String): Inet4Address = InetAddress.getByName(value) as Inet4Address

private fun apkServerThreads(): List<Thread> = Thread.getAllStackTraces().keys.filter { thread ->
    thread.isAlive &&
        (thread.name == "apk-share-accept" || thread.name.startsWith("apk-share-client-"))
}

private fun assertEventually(
    message: String,
    timeoutMillis: Long = 2_500,
    condition: () -> Boolean,
) {
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    while (System.nanoTime() < deadline) {
        if (condition()) return
        Thread.yield()
    }
    if (!condition()) fail(message)
}
