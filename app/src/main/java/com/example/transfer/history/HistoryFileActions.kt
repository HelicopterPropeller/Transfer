package com.example.transfer.history

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import androidx.core.net.toUri
import com.example.transfer.R
import java.io.FileNotFoundException
import java.io.IOException

internal enum class HistoryOpenError {
    FILE_UNAVAILABLE,
    NO_HANDLER
}

internal interface HistoryFileOpener {
    fun ensureReadable(uri: String)
    fun hasViewHandler(uri: String, mimeType: String): Boolean
    fun open(uri: String, mimeType: String)
}

object HistoryFileActions {
    fun open(context: Context, entry: TransferHistoryEntry): String? {
        val error = open(entry, AndroidHistoryFileOpener(context)) ?: return null
        return context.errorMessage(error)
    }

    fun open(context: Context, entry: HistoryItemUi): String? {
        val error = open(
            allowed = entry.showOpen,
            receivedUri = entry.receivedUri,
            mimeType = entry.mimeType,
            opener = AndroidHistoryFileOpener(context)
        ) ?: return null
        return context.errorMessage(error)
    }

    private fun Context.errorMessage(error: HistoryOpenError): String =
        getString(
            when (error) {
                HistoryOpenError.FILE_UNAVAILABLE -> R.string.history_file_unavailable
                HistoryOpenError.NO_HANDLER -> R.string.history_no_open_app
            }
        )

    internal fun open(
        entry: TransferHistoryEntry,
        opener: HistoryFileOpener
    ): HistoryOpenError? = open(
        allowed = entry.direction == TransferDirection.RECEIVE &&
            entry.status == TransferHistoryStatus.SUCCESS,
        receivedUri = entry.receivedUri,
        mimeType = entry.mimeType,
        opener = opener
    )

    private fun open(
        allowed: Boolean,
        receivedUri: String?,
        mimeType: String,
        opener: HistoryFileOpener
    ): HistoryOpenError? {
        val uri = receivedUri?.takeIf(String::isNotBlank)
        if (!allowed || uri == null) return HistoryOpenError.FILE_UNAVAILABLE
        val resolvedMimeType = mimeType.ifBlank { DEFAULT_MIME_TYPE }
        return try {
            opener.ensureReadable(uri)
            if (!opener.hasViewHandler(uri, resolvedMimeType)) {
                HistoryOpenError.NO_HANDLER
            } else {
                opener.open(uri, resolvedMimeType)
                null
            }
        } catch (_: ActivityNotFoundException) {
            HistoryOpenError.NO_HANDLER
        } catch (_: SecurityException) {
            HistoryOpenError.FILE_UNAVAILABLE
        } catch (_: FileNotFoundException) {
            HistoryOpenError.FILE_UNAVAILABLE
        } catch (_: IOException) {
            HistoryOpenError.FILE_UNAVAILABLE
        } catch (_: IllegalArgumentException) {
            HistoryOpenError.FILE_UNAVAILABLE
        }
    }

    /*
     * Keep the Android adapter below the pure policy so local unit tests can verify all decisions
     * without relying on framework stubs.
     */
    private class AndroidHistoryFileOpener(
        private val context: Context
    ) : HistoryFileOpener {
        override fun ensureReadable(uri: String) {
            context.contentResolver.openFileDescriptor(uri.toUri(), "r")?.use { }
                ?: throw FileNotFoundException(uri)
        }

        override fun hasViewHandler(uri: String, mimeType: String): Boolean =
            viewIntent(uri, mimeType).resolveActivity(context.packageManager) != null

        override fun open(uri: String, mimeType: String) {
            context.startActivity(viewIntent(uri, mimeType))
        }

        private fun viewIntent(uri: String, mimeType: String) =
            Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(uri.toUri(), mimeType.ifBlank { DEFAULT_MIME_TYPE })
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }
    }

    private const val DEFAULT_MIME_TYPE = "application/octet-stream"
}
