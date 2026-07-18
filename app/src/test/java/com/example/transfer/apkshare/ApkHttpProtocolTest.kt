package com.example.transfer.apkshare

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.charset.StandardCharsets.UTF_8
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class ApkHttpProtocolTest {
    @get:Rule
    val temporary = TemporaryFolder()

    @Test
    fun `valid landing page escapes artifact fields and links only to token download`() {
        val artifact = artifactContaining(
            content = "apk",
            fileName = "Transfer-<script>&\"'.apk",
            versionName = "1<&\"'>",
            sha256 = "hash<&\"'>",
        )
        val session = fixedSession()
        val output = ByteArrayOutputStream()

        ApkHttpProtocol(artifact, session).respond(
            request(target = "/i/${session.token}/"),
            output,
        )

        val response = output.toByteArray().httpResponse()
        val html = response.body.toString(UTF_8)
        assertEquals("HTTP/1.1 200 OK", response.statusLine)
        assertEquals("text/html; charset=utf-8", response.headers["Content-Type"])
        assertEquals(response.body.size.toString(), response.headers["Content-Length"])
        assertEquals("no-store", response.headers["Cache-Control"])
        assertEquals("nosniff", response.headers["X-Content-Type-Options"])
        assertEquals("close", response.headers["Connection"])
        assertTrue(html.contains("Transfer-&lt;script&gt;&amp;&quot;&#39;.apk"))
        assertTrue(html.contains("1&lt;&amp;&quot;&#39;&gt;"))
        assertTrue(html.contains("hash&lt;&amp;&quot;&#39;&gt;"))
        assertFalse(html.contains("<script>"))
        assertFalse(html.contains(artifact.versionName))
        assertFalse(html.contains(artifact.sha256))
        assertEquals(
            listOf("/i/${session.token}/app.apk"),
            Regex("href=\"([^\"]+)\"").findAll(html).map { it.groupValues[1] }.toList(),
        )
        assertTrue(session.authorize(session.token))
    }

    @Test
    fun `valid download writes exact headers and body then consumes session`() {
        val body = "apk-body".toByteArray(UTF_8)
        val artifact = artifactContaining(body)
        val session = fixedSession()
        val output = ByteArrayOutputStream()

        ApkHttpProtocol(artifact, session).respond(
            request(target = "/i/${session.token}/app.apk"),
            output,
        )

        val expectedHeaders = buildString {
            append("HTTP/1.1 200 OK\r\n")
            append("Content-Type: application/vnd.android.package-archive\r\n")
            append("Content-Length: ${body.size}\r\n")
            append("Connection: close\r\n")
            append("Cache-Control: no-store\r\n")
            append("X-Content-Type-Options: nosniff\r\n")
            append("Content-Disposition: attachment; filename=\"Transfer-test.apk\"\r\n")
            append("\r\n")
        }.toByteArray(UTF_8)
        val expected = expectedHeaders + body
        assertArrayEquals(expected, output.toByteArray())
        assertFalse(session.authorize(session.token))
    }

    @Test
    fun `invalid token is forbidden without exposing artifact or consuming session`() {
        val session = fixedSession()
        val output = ByteArrayOutputStream()

        ApkHttpProtocol(artifactContaining("secret"), session).respond(
            request(target = "/i/${"0".repeat(48)}/app.apk"),
            output,
        )

        assertEquals("HTTP/1.1 403 Forbidden", output.toByteArray().httpResponse().statusLine)
        assertFalse(output.toString(UTF_8.name()).contains("secret"))
        assertTrue(session.authorize(session.token))
    }

    @Test
    fun `expired token is forbidden without exposing artifact`() {
        var now = 1_000L
        val session = fixedSession { now }
        now = session.expiresAtMillis
        val output = ByteArrayOutputStream()

        ApkHttpProtocol(artifactContaining("secret"), session).respond(
            request(target = "/i/${session.token}/app.apk"),
            output,
        )

        assertEquals("HTTP/1.1 403 Forbidden", output.toByteArray().httpResponse().statusLine)
        assertFalse(output.toString(UTF_8.name()).contains("secret"))
    }

    @Test
    fun `consumed token is forbidden on another download`() {
        val session = fixedSession()
        val protocol = ApkHttpProtocol(artifactContaining("one-shot"), session)
        protocol.respond(
            request(target = "/i/${session.token}/app.apk"),
            ByteArrayOutputStream(),
        )
        val second = ByteArrayOutputStream()

        protocol.respond(request(target = "/i/${session.token}/app.apk"), second)

        assertEquals("HTTP/1.1 403 Forbidden", second.toByteArray().httpResponse().statusLine)
        assertFalse(second.toString(UTF_8.name()).contains("one-shot"))
    }

    @Test
    fun `non GET method is rejected without exposing artifact or consuming session`() {
        val session = fixedSession()
        val output = ByteArrayOutputStream()

        ApkHttpProtocol(artifactContaining("secret"), session).respond(
            request(method = "POST", target = "/i/${session.token}/app.apk"),
            output,
        )

        assertEquals("HTTP/1.1 405 Method Not Allowed", output.toByteArray().httpResponse().statusLine)
        assertFalse(output.toString(UTF_8.name()).contains("secret"))
        assertTrue(session.authorize(session.token))
    }

    @Test
    fun `malformed paths are not found without exposing artifact or consuming session`() {
        val session = fixedSession()
        val malformedTargets = listOf(
            "/i/${session.token}/app.apk/extra",
            "/i/${session.token}/APP.apk",
            "/i/${session.token}/../app.apk",
            "/i/${session.token}x/app.apk",
            "/i/${session.token}/app.apk?download=1",
        )

        malformedTargets.forEach { target ->
            val output = ByteArrayOutputStream()
            ApkHttpProtocol(artifactContaining("secret"), session).respond(
                request(target = target),
                output,
            )
            assertEquals(
                target,
                "HTTP/1.1 404 Not Found",
                output.toByteArray().httpResponse().statusLine,
            )
            assertFalse(output.toString(UTF_8.name()).contains("secret"))
        }
        assertTrue(session.authorize(session.token))
    }

    @Test
    fun `Range header is rejected case insensitively without consuming session`() {
        val session = fixedSession()
        val output = ByteArrayOutputStream()

        ApkHttpProtocol(artifactContaining("secret"), session).respond(
            request(
                target = "/i/${session.token}/app.apk",
                headers = mapOf("rAnGe" to "bytes=0-1"),
            ),
            output,
        )

        assertEquals(
            "HTTP/1.1 416 Range Not Satisfiable",
            output.toByteArray().httpResponse().statusLine,
        )
        assertFalse(output.toString(UTF_8.name()).contains("secret"))
        assertTrue(session.authorize(session.token))
    }

    @Test
    fun `second concurrent download attempt is rejected without disturbing active attempt`() {
        val session = fixedSession()
        val activeAttempt = session.beginAttempt(session.token)!!
        val protocol = ApkHttpProtocol(artifactContaining("secret"), session)
        val output = ByteArrayOutputStream()

        protocol.respond(request(target = "/i/${session.token}/app.apk"), output)

        assertEquals("HTTP/1.1 409 Conflict", output.toByteArray().httpResponse().statusLine)
        assertFalse(output.toString(UTF_8.name()).contains("secret"))
        session.failAttempt(activeAttempt)
        val retry = ByteArrayOutputStream()
        protocol.respond(request(target = "/i/${session.token}/app.apk"), retry)
        assertEquals("HTTP/1.1 200 OK", retry.toByteArray().httpResponse().statusLine)
    }

    @Test
    fun `write failure releases attempt so a complete download can retry`() {
        val body = ByteArray(4_096) { index -> (index % 251).toByte() }
        val session = fixedSession()
        val protocol = ApkHttpProtocol(artifactContaining(body), session)

        try {
            protocol.respond(
                request(target = "/i/${session.token}/app.apk"),
                FailingOutputStream(bytesBeforeFailure = 512),
            )
            fail("Expected the interrupted response to throw")
        } catch (_: IOException) {
            // Expected: the protocol must release its attempt before propagating the write failure.
        }

        assertTrue(session.authorize(session.token))
        val retry = ByteArrayOutputStream()
        protocol.respond(request(target = "/i/${session.token}/app.apk"), retry)
        val retryResponse = retry.toByteArray().httpResponse()
        assertEquals("HTTP/1.1 200 OK", retryResponse.statusLine)
        assertArrayEquals(body, retryResponse.body)
        assertFalse(session.authorize(session.token))
    }

    private fun request(
        method: String = "GET",
        target: String,
        version: String = "HTTP/1.1",
        headers: Map<String, String> = emptyMap(),
    ): HttpRequest = HttpRequest(method, target, version, headers)

    private fun artifactContaining(
        content: String,
        fileName: String = "Transfer-test.apk",
        versionName: String = "test",
        sha256: String = "0".repeat(64),
    ): ApkArtifact = artifactContaining(content.toByteArray(UTF_8), fileName, versionName, sha256)

    private fun artifactContaining(
        content: ByteArray,
        fileName: String = "Transfer-test.apk",
        versionName: String = "test",
        sha256: String = "0".repeat(64),
    ): ApkArtifact {
        val file = temporary.newFile().apply { writeBytes(content) }
        return ApkArtifact(file, fileName, versionName, 1, file.length(), sha256)
    }

    private fun fixedSession(nowMillis: () -> Long = { 1_000L }) = ApkShareSession.create(
        nowMillis = nowMillis,
        randomBytes = { size -> ByteArray(size) { 7 } },
    )
}

private class FailingOutputStream(private var bytesBeforeFailure: Int) : OutputStream() {
    override fun write(value: Int) {
        if (bytesBeforeFailure-- <= 0) throw IOException("client disconnected")
    }

    override fun write(bytes: ByteArray, offset: Int, length: Int) {
        if (length > bytesBeforeFailure) {
            bytesBeforeFailure = 0
            throw IOException("client disconnected")
        }
        bytesBeforeFailure -= length
    }
}

private data class ParsedHttpResponse(
    val statusLine: String,
    val headers: Map<String, String>,
    val body: ByteArray,
)

private fun ByteArray.httpResponse(): ParsedHttpResponse {
    val headerEnd = indexOfHeaderEnd()
    val lines = String(this, 0, headerEnd - 4, UTF_8).split("\r\n")
    return ParsedHttpResponse(
        statusLine = lines.first(),
        headers = lines.drop(1).associate { line ->
            val separator = line.indexOf(':')
            check(separator > 0) { "Malformed response header: $line" }
            line.substring(0, separator) to line.substring(separator + 1).trim()
        },
        body = copyOfRange(headerEnd, size),
    )
}

private fun ByteArray.indexOfHeaderEnd(): Int {
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
