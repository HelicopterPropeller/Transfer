package com.example.transfer.storage

import java.io.File

internal object LegacyDownloadFiles {
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
