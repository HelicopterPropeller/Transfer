package com.example.transfer.ui

import android.content.Context
import android.content.Intent
import android.net.Uri
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
        val metadata = context.contentResolver.query(
            uri,
            arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE),
            null,
            null,
            null
        )?.use { cursor ->
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
            name?.takeIf(String::isNotBlank)?.let { it to size }
        } ?: return null
        if (metadata.second < 0) return null
        context.contentResolver.openFileDescriptor(uri, "r")?.use { } ?: return null
        return SelectedFile(
            uri = uri.toString(),
            displayName = metadata.first,
            mimeType = context.contentResolver.getType(uri).orEmpty()
                .ifBlank { DEFAULT_MIME_TYPE },
            size = metadata.second
        )
    }

    private companion object {
        const val DEFAULT_MIME_TYPE = "application/octet-stream"
    }
}
