package com.example.transfer.ui

import android.content.Context
import android.content.Intent
import android.net.Uri
import android.provider.DocumentsContract
import android.provider.MediaStore
import android.provider.OpenableColumns

class SelectedFileResolver(
    private val context: Context
) {
    fun resolve(uri: Uri, persistPermission: Boolean): SelectedFile? = runCatching {
        validateThenPersist(
            validate = { validate(uri) },
            persist = {
                if (persistPermission) {
                    context.contentResolver.takePersistableUriPermission(
                        uri,
                        Intent.FLAG_GRANT_READ_URI_PERMISSION
                    )
                }
            }
        )
    }.getOrNull()

    private fun validate(uri: Uri): SelectedFile? {
        val metadata = stat(uri) ?: return null
        return SelectedFile(
            uri = uri.toString(),
            displayName = metadata.displayName,
            mimeType = context.contentResolver.getType(uri).orEmpty()
                .ifBlank { DEFAULT_MIME_TYPE },
            size = metadata.size,
            lastModified = metadata.lastModified
        )
    }

    fun stat(uri: Uri): SelectedFileMetadata? {
        val resolver = context.contentResolver
        val projections = listOf(
            arrayOf(
                OpenableColumns.DISPLAY_NAME,
                OpenableColumns.SIZE,
                DocumentsContract.Document.COLUMN_LAST_MODIFIED,
                MediaStore.MediaColumns.DATE_MODIFIED
            ),
            arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE)
        )
        val metadata = projections.firstNotNullOfOrNull { projection ->
            runCatching { resolver.query(uri, projection, null, null, null) }.getOrNull()
                ?.use { cursor ->
            if (!cursor.moveToFirst()) return@use null
            val nameIndex = cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME)
            val sizeIndex = cursor.getColumnIndex(OpenableColumns.SIZE)
            val name = if (nameIndex >= 0 && !cursor.isNull(nameIndex)) {
                cursor.getString(nameIndex)
            } else {
                null
            }
            val size = if (sizeIndex >= 0 && !cursor.isNull(sizeIndex)) {
                cursor.getLong(sizeIndex)
            } else {
                -1L
            }
            val documentModified = cursor.longOrNull(
                DocumentsContract.Document.COLUMN_LAST_MODIFIED
            )
            val mediaModifiedSeconds = cursor.longOrNull(MediaStore.MediaColumns.DATE_MODIFIED)
            name?.takeIf(String::isNotBlank)?.let {
                SelectedFileMetadata(
                    displayName = it,
                    size = size,
                    lastModified = documentModified ?: mediaModifiedSeconds?.let(::secondsToMillis)
                )
            }
        }
        } ?: return null
        if (metadata.size < 0) return null
        resolver.openFileDescriptor(uri, "r")?.use { } ?: return null
        return metadata
    }

    private fun android.database.Cursor.longOrNull(column: String): Long? {
        val index = getColumnIndex(column)
        return if (index >= 0 && !isNull(index)) getLong(index) else null
    }

    private fun secondsToMillis(seconds: Long): Long = try {
        Math.multiplyExact(seconds, 1_000L)
    } catch (_: ArithmeticException) {
        Long.MAX_VALUE
    }

    private companion object {
        const val DEFAULT_MIME_TYPE = "application/octet-stream"
    }
}

data class SelectedFileMetadata(
    val displayName: String,
    val size: Long,
    val lastModified: Long?
)
