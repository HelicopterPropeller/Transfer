package com.example.transfer.storage

import java.io.Closeable
import java.io.InputStream

data class StoredFileLocation(
    val kind: String,
    val value: String
) {
    companion object {
        const val MEDIA_STORE = "MEDIA_STORE"
        const val LEGACY_FILE = "LEGACY_FILE"
    }
}

interface ResumableFileHandle : Closeable {
    val location: StoredFileLocation
    val displayName: String

    fun length(): Long
    fun writeAt(offset: Long, source: ByteArray, length: Int)
    fun truncate(length: Long)
    fun force()
}

interface ResumableIncomingFileStore {
    suspend fun create(
        transferId: String,
        fileName: String,
        mimeType: String
    ): ResumableFileHandle

    /** Finds the deterministic pending file for a persisted staging id after process death. */
    suspend fun findStaging(transferId: String): StoredFileLocation? = null

    suspend fun reopen(
        location: StoredFileLocation,
        displayName: String
    ): ResumableFileHandle?
    suspend fun openInput(location: StoredFileLocation): InputStream?
    /** Opens the committed bytes whether the resumable location is still pending or already published. */
    suspend fun openCompletionInput(
        location: StoredFileLocation,
        displayName: String
    ): InputStream? = openInput(location)
    suspend fun publish(handle: ResumableFileHandle): String?
    /** Returns the result of an already-completed publish without publishing a second file. */
    suspend fun recoverPublished(location: StoredFileLocation, displayName: String): String? = null
    suspend fun delete(location: StoredFileLocation)
}
