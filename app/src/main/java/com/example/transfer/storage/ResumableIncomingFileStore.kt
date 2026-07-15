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

    suspend fun reopen(
        location: StoredFileLocation,
        displayName: String
    ): ResumableFileHandle?
    suspend fun openInput(location: StoredFileLocation): InputStream?
    suspend fun publish(handle: ResumableFileHandle): String?
    suspend fun delete(location: StoredFileLocation)
}
