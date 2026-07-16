package com.example.transfer.storage

import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.security.MessageDigest
import java.security.SecureRandom

internal object LegacyDownloadFiles {
    fun stablePublishedName(requestedName: String, partialIdentity: String): String {
        val safeName = FileNamePolicy.sanitize(requestedName)
        val dot = safeName.lastIndexOf('.').takeIf { it > 0 } ?: safeName.length
        val base = safeName.substring(0, dot)
        val extension = safeName.substring(dot)
        val stableSuffix = stableSuffix(partialIdentity)
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

    @Synchronized
    fun publishWithoutOverwrite(
        partial: File,
        requestedName: String,
        expectedDigest: ByteArray,
        nonceFactory: () -> String = ::newPublishNonce,
        moveIntoPlace: (source: File, target: File) -> Boolean
    ): File {
        require(expectedDigest.size == SHA_256_SIZE) { "Invalid whole-file digest" }
        check(partial.isFile) { "Partial download no longer exists" }
        check(matchesDigest(partial, expectedDigest)) { "Partial download digest changed" }

        recoverPublished(partial, requestedName, expectedDigest)?.let { return it }
        for (attempt in 0 until MAX_PUBLISH_ATTEMPTS) {
            val candidate = uniquePublishedCandidate(
                partial, requestedName, expectedDigest, nonceFactory()
            )
            if (!moveIntoPlace(partial, candidate)) {
                if (candidate.isFile && matchesDigest(candidate, expectedDigest)) {
                    if (partial.exists()) {
                        check(partial.delete()) { "Unable to remove published partial file" }
                    }
                    return candidate
                }
                continue
            }
            check(!partial.exists() && candidate.isFile) { "Atomic publish move did not complete" }
            return candidate
        }
        throw IOException("Unable to reserve a published download name")
    }

    @Synchronized
    fun recoverPublished(
        partial: File,
        requestedName: String,
        expectedDigest: ByteArray
    ): File? {
        require(expectedDigest.size == SHA_256_SIZE) { "Invalid whole-file digest" }
        val directory = checkNotNull(partial.canonicalFile.parentFile) {
            "Partial download has no parent directory"
        }
        val marker = uniqueNameMarker(partial, expectedDigest)
        val currentCandidates = directory.listFiles()
            .orEmpty()
            .asSequence()
            .filter { it.name.contains(marker) }
            .sortedBy(File::getName)
        val candidates = sequenceOf(legacyPublishedCandidate(partial, requestedName)) +
            currentCandidates
        for (candidate in candidates) {
            if (!candidate.isFile || !matchesDigest(candidate, expectedDigest)) continue
            if (partial.exists()) {
                if (!partial.isFile || !matchesDigest(partial, expectedDigest)) return null
                check(partial.delete()) { "Unable to remove published partial file" }
            }
            return candidate
        }
        return null
    }

    private fun legacyPublishedCandidate(partial: File, requestedName: String): File {
        val directory = checkNotNull(partial.canonicalFile.parentFile) {
            "Partial download has no parent directory"
        }
        return requireContained(
            directory,
            File(directory, stablePublishedName(requestedName, partial.canonicalPath)).path
        )
    }

    private fun uniquePublishedCandidate(
        partial: File,
        requestedName: String,
        expectedDigest: ByteArray,
        nonce: String
    ): File {
        val directory = checkNotNull(partial.canonicalFile.parentFile) {
            "Partial download has no parent directory"
        }
        require(nonce.length == NONCE_NAME_BYTES * 2 && nonce.all { it in "0123456789abcdef" }) {
            "Invalid publish nonce"
        }
        val name = FileNamePolicy.withTimestamp(
            requestedName,
            "${uniqueNameMarker(partial, expectedDigest)}_$nonce",
            1
        )
        return requireContained(directory, File(directory, name).path)
    }

    private fun uniqueNameMarker(partial: File, expectedDigest: ByteArray): String {
        val identity = MessageDigest.getInstance("SHA-256")
            .digest(partial.canonicalPath.toByteArray(Charsets.UTF_8))
            .take(IDENTITY_NAME_BYTES)
            .toHex()
        val digest = expectedDigest.take(DIGEST_NAME_BYTES).toHex()
        return "${identity}_$digest"
    }

    private fun newPublishNonce(): String = ByteArray(NONCE_NAME_BYTES)
        .also(SecureRandom()::nextBytes)
        .asList()
        .toHex()

    private fun matchesDigest(file: File, expectedDigest: ByteArray): Boolean =
        MessageDigest.isEqual(expectedDigest, sha256(file))

    fun sha256(file: File): ByteArray {
        val digest = MessageDigest.getInstance("SHA-256")
        FileInputStream(file).use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
        }
        return digest.digest()
    }

    private fun stableSuffix(partialIdentity: String): String =
        MessageDigest.getInstance("SHA-256")
            .digest(partialIdentity.toByteArray(Charsets.UTF_8))
            .take(6)
            .toHex()

    private fun List<Byte>.toHex(): String =
        joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }

    private const val SHA_256_SIZE = 32
    private const val DIGEST_NAME_BYTES = 12
    private const val IDENTITY_NAME_BYTES = 16
    private const val NONCE_NAME_BYTES = 16
    private const val MAX_PUBLISH_ATTEMPTS = 1_000
}
