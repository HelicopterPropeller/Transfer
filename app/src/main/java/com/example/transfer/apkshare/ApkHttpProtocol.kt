package com.example.transfer.apkshare

import java.io.EOFException
import java.io.IOException
import java.io.OutputStream
import java.nio.charset.StandardCharsets.UTF_8

data class HttpRequest(
    val method: String,
    val target: String,
    val version: String,
    val headers: Map<String, String> = emptyMap(),
)

class ApkHttpProtocol(
    private val artifact: ApkArtifact,
    private val session: ApkShareSession,
    private val onDownloadFailure: (Exception) -> Unit = {},
) {
    fun respond(request: HttpRequest, output: OutputStream) {
        if (request.method != "GET") {
            writeError(output, "405 Method Not Allowed")
            return
        }
        if (request.version != "HTTP/1.0" && request.version != "HTTP/1.1") {
            writeError(output, "400 Bad Request")
            return
        }
        if (request.headers.keys.any { it.equals("Range", ignoreCase = true) }) {
            writeError(output, "416 Range Not Satisfiable")
            return
        }

        val downloadMatch = DOWNLOAD.matchEntire(request.target)
        if (downloadMatch != null) {
            respondWithDownload(downloadMatch.groupValues[1], output)
            return
        }

        val landingMatch = LANDING.matchEntire(request.target)
        if (landingMatch != null) {
            respondWithLanding(landingMatch.groupValues[1], output)
            return
        }

        writeError(output, "404 Not Found")
    }

    private fun respondWithLanding(candidate: String, output: OutputStream) {
        if (!session.authorize(candidate)) {
            writeError(output, "403 Forbidden")
            return
        }

        val body = landingHtml(candidate).toByteArray(UTF_8)
        output.write(headers("200 OK", "text/html; charset=utf-8", body.size.toLong()))
        output.write(body)
        output.flush()
    }

    private fun respondWithDownload(candidate: String, output: OutputStream) {
        if (!session.authorize(candidate)) {
            writeError(output, "403 Forbidden")
            return
        }

        val attempt = session.beginAttempt(candidate)
        if (attempt == null) {
            val status = if (session.authorize(candidate)) "409 Conflict" else "403 Forbidden"
            writeError(output, status)
            return
        }

        var completed = false
        try {
            output.write(
                headers(
                    status = "200 OK",
                    type = "application/vnd.android.package-archive",
                    length = artifact.size,
                    disposition = "attachment; filename=\"${dispositionFileName(artifact.fileName)}\"",
                ),
            )
            writeArtifact(output)
            output.flush()
            completed = session.completeAttempt(attempt)
            if (!completed) {
                throw IOException("Download session expired before completion")
            }
        } catch (exception: Exception) {
            // Publish the terminal event before releasing this attempt. A retry may
            // begin as soon as failAttempt runs, so reversing these calls can make
            // the next start appear before this failure to a serialized listener.
            runCatching { onDownloadFailure(exception) }
            throw exception
        } finally {
            if (!completed) session.failAttempt(attempt)
        }
    }

    private fun writeArtifact(output: OutputStream) {
        require(artifact.size >= 0L) { "Artifact size must not be negative" }
        artifact.file.inputStream().use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            var remaining = artifact.size
            while (remaining > 0L) {
                val read = input.read(buffer, 0, minOf(buffer.size.toLong(), remaining).toInt())
                if (read < 0) {
                    throw EOFException("Artifact ended before its declared size")
                }
                if (read == 0) {
                    throw IOException("Artifact read made no progress")
                }
                output.write(buffer, 0, read)
                remaining -= read
            }
        }
    }

    private fun landingHtml(candidate: String): String {
        val fileName = htmlEscape(artifact.fileName)
        val versionName = htmlEscape(artifact.versionName)
        val sha256 = htmlEscape(artifact.sha256)
        return """
            <!doctype html>
            <html lang="en">
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1">
              <title>Download $fileName</title>
            </head>
            <body>
              <main>
                <h1>$fileName</h1>
                <p>Version: $versionName (${artifact.versionCode})</p>
                <p>Size: ${artifact.size} bytes</p>
                <p>SHA-256: $sha256</p>
                <p><a href="/i/$candidate/app.apk">Download APK</a></p>
              </main>
            </body>
            </html>
        """.trimIndent()
    }

    private fun htmlEscape(value: String): String = buildString(value.length) {
        value.forEach { character ->
            append(
                when (character) {
                    '&' -> "&amp;"
                    '<' -> "&lt;"
                    '>' -> "&gt;"
                    '"' -> "&quot;"
                    '\'' -> "&#39;"
                    else -> character
                },
            )
        }
    }

    private fun dispositionFileName(value: String): String = buildString(value.length) {
        value.forEach { character ->
            append(
                if (character == '"' || character == '\\' || character.code < 0x20 || character.code == 0x7f) {
                    '_'
                } else {
                    character
                },
            )
        }
    }

    private fun writeError(output: OutputStream, status: String) {
        val body = "$status\n".toByteArray(UTF_8)
        output.write(headers(status, "text/plain; charset=utf-8", body.size.toLong()))
        output.write(body)
        output.flush()
    }

    private fun headers(
        status: String,
        type: String,
        length: Long,
        disposition: String? = null,
    ): ByteArray = buildString {
        append("HTTP/1.1 $status\r\n")
        append("Content-Type: $type\r\n")
        append("Content-Length: $length\r\n")
        append("Connection: close\r\n")
        append("Cache-Control: no-store\r\n")
        append("X-Content-Type-Options: nosniff\r\n")
        if (disposition != null) append("Content-Disposition: $disposition\r\n")
        append("\r\n")
    }.toByteArray(UTF_8)

    private companion object {
        val LANDING = Regex("^/i/([a-f0-9]{48})/?$")
        val DOWNLOAD = Regex("^/i/([a-f0-9]{48})/app\\.apk$")
    }
}
