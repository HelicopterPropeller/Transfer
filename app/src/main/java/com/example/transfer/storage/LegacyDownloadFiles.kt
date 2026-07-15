package com.example.transfer.storage

import java.io.File
import java.security.MessageDigest

internal object LegacyDownloadFiles {
    fun stablePublishedName(requestedName: String, partialIdentity: String): String {
        val safeName = FileNamePolicy.sanitize(requestedName)
        val dot = safeName.lastIndexOf('.').takeIf { it > 0 } ?: safeName.length
        val base = safeName.substring(0, dot)
        val extension = safeName.substring(dot)
        val stableSuffix = MessageDigest.getInstance("SHA-256")
            .digest(partialIdentity.toByteArray(Charsets.UTF_8))
            .take(6)
            .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
        return "${base}_$stableSuffix$extension"
    }

    fun requireContained(directory: File, persistedPath: String): File {
        val canonicalDirectory = directory.canonicalFile
        val file = File(persistedPath).canonicalFile
        if (file.parentFile != canonicalDirectory) {
            throw SecurityException("Invalid partial download path")
        }
        return file
    }

    fun reserveFinalFile(
        directory: File,
        requestedName: String,
        timestamp: () -> String
    ): File {
        val canonicalDirectory = directory.canonicalFile
        val safeName = FileNamePolicy.sanitize(requestedName)
        val requested = File(canonicalDirectory, safeName)
        if (requested.createNewFile()) return requested

        val stamp = timestamp()
        var attempt = 1
        while (true) {
            val candidate = File(
                canonicalDirectory,
                FileNamePolicy.withTimestamp(safeName, stamp, attempt++)
            )
            if (candidate.createNewFile()) return candidate
        }
    }
}
