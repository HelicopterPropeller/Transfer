package com.example.transfer.storage

import android.Manifest
import android.content.ContentValues
import android.content.Context
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.provider.MediaStore
import androidx.annotation.RequiresApi
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

data class ReceivedFileHandle(
    val output: OutputStream,
    val uri: Uri? = null,
    val file: File? = null,
    val displayName: String
)

interface IncomingFileStore {
    suspend fun create(fileName: String, mimeType: String): ReceivedFileHandle
    suspend fun complete(handle: ReceivedFileHandle): String?
    suspend fun abort(handle: ReceivedFileHandle)
}

class DownloadStorage(private val context: Context) : IncomingFileStore {
    override suspend fun create(fileName: String, mimeType: String): ReceivedFileHandle =
        withContext(Dispatchers.IO) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                createMediaStoreFile(fileName, mimeType)
            } else {
                createLegacyFile(fileName)
            }
        }

    override suspend fun complete(handle: ReceivedFileHandle): String? = withContext(Dispatchers.IO) {
        handle.output.flush()
        handle.output.close()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && handle.uri != null) {
            val values = ContentValues().apply { put(MediaStore.MediaColumns.IS_PENDING, 0) }
            context.contentResolver.update(handle.uri, values, null, null)
            handle.uri.toString()
        } else {
            FileProvider.getUriForFile(
                context,
                "${context.packageName}.files",
                requireNotNull(handle.file)
            ).toString()
        }
    }

    override suspend fun abort(handle: ReceivedFileHandle) = withContext(Dispatchers.IO) {
        runCatching { handle.output.close() }
        when {
            handle.uri != null -> context.contentResolver.delete(handle.uri, null, null)
            handle.file != null -> handle.file.delete()
        }
        Unit
    }

    @RequiresApi(Build.VERSION_CODES.Q)
    private fun createMediaStoreFile(requestedName: String, mimeType: String): ReceivedFileHandle {
        val resolver = context.contentResolver
        val relativePath = "${Environment.DIRECTORY_DOWNLOADS}/Transfer"
        val displayName = uniqueMediaStoreName(FileNamePolicy.sanitize(requestedName), relativePath)
        val values = ContentValues().apply {
            put(MediaStore.MediaColumns.DISPLAY_NAME, displayName)
            put(MediaStore.MediaColumns.MIME_TYPE, mimeType.ifBlank { "application/octet-stream" })
            put(MediaStore.MediaColumns.RELATIVE_PATH, relativePath)
            put(MediaStore.MediaColumns.IS_PENDING, 1)
        }
        val uri = resolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values)
            ?: error("无法在 Download/Transfer 创建文件")
        val output = resolver.openOutputStream(uri, "w") ?: run {
            resolver.delete(uri, null, null)
            error("无法打开接收文件")
        }
        return ReceivedFileHandle(output, uri = uri, displayName = displayName)
    }

    @RequiresApi(Build.VERSION_CODES.Q)
    private fun uniqueMediaStoreName(name: String, relativePath: String): String {
        if (!mediaStoreNameExists(name, relativePath)) return name
        val timestamp = timestamp()
        var attempt = 1
        while (true) {
            val candidate = FileNamePolicy.withTimestamp(name, timestamp, attempt++)
            if (!mediaStoreNameExists(candidate, relativePath)) return candidate
        }
    }

    @RequiresApi(Build.VERSION_CODES.Q)
    private fun mediaStoreNameExists(name: String, relativePath: String): Boolean {
        val projection = arrayOf(MediaStore.MediaColumns._ID)
        val selection = "${MediaStore.MediaColumns.DISPLAY_NAME}=? AND ${MediaStore.MediaColumns.RELATIVE_PATH}=?"
        context.contentResolver.query(
            MediaStore.Downloads.EXTERNAL_CONTENT_URI,
            projection,
            selection,
            arrayOf(name, "$relativePath/"),
            null
        )?.use { return it.moveToFirst() }
        return false
    }

    @Suppress("DEPRECATION")
    private fun createLegacyFile(requestedName: String): ReceivedFileHandle {
        check(
            ContextCompat.checkSelfPermission(context, Manifest.permission.WRITE_EXTERNAL_STORAGE) ==
                PackageManager.PERMISSION_GRANTED
        ) { "没有存储权限，无法保存到 Download/Transfer" }
        val directory = File(
            Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS),
            "Transfer"
        )
        check(directory.exists() || directory.mkdirs()) { "无法创建 Download/Transfer" }
        val safeName = FileNamePolicy.sanitize(requestedName)
        var file = File(directory, safeName)
        if (file.exists()) {
            val stamp = timestamp()
            var attempt = 1
            do {
                file = File(directory, FileNamePolicy.withTimestamp(safeName, stamp, attempt++))
            } while (file.exists())
        }
        return ReceivedFileHandle(FileOutputStream(file), file = file, displayName = file.name)
    }

    private fun timestamp(): String =
        SimpleDateFormat("yyyyMMdd_HHmmss", Locale.US).format(Date())
}
