package com.example.transfer.storage

import android.Manifest
import android.content.ContentResolver
import android.content.ContentValues
import android.content.Context
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.os.ParcelFileDescriptor
import android.provider.MediaStore
import androidx.annotation.RequiresApi
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.InputStream
import java.io.IOException
import java.io.OutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
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

class DownloadStorage(private val context: Context) :
    IncomingFileStore,
    ResumableIncomingFileStore {

    override suspend fun create(fileName: String, mimeType: String): ReceivedFileHandle =
        withContext(Dispatchers.IO) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                createMediaStoreFile(fileName, mimeType)
            } else {
                createLegacyFile(fileName)
            }
        }

    override suspend fun create(
        transferId: String,
        fileName: String,
        mimeType: String
    ): ResumableFileHandle = withContext(Dispatchers.IO) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            createMediaStoreResumableFile(fileName, mimeType)
        } else {
            createLegacyResumableFile(transferId, fileName)
        }
    }

    override suspend fun reopen(
        location: StoredFileLocation,
        displayName: String
    ): ResumableFileHandle? =
        withContext(Dispatchers.IO) {
            try {
                when (location.kind) {
                    StoredFileLocation.MEDIA_STORE -> {
                        val uri = ownedPendingMediaStoreUri(location) ?: return@withContext null
                        openMediaStoreHandle(uri, FileNamePolicy.sanitize(displayName))
                    }
                    StoredFileLocation.LEGACY_FILE -> reopenLegacyFile(location, displayName)
                    else -> null
                }
            } catch (_: FileNotFoundException) {
                null
            }
        }

    override suspend fun openInput(location: StoredFileLocation): InputStream? =
        withContext(Dispatchers.IO) {
            when (location.kind) {
                StoredFileLocation.MEDIA_STORE -> {
                    val uri = ownedPendingMediaStoreUri(location) ?: return@withContext null
                    try {
                        context.contentResolver.openInputStream(uri)
                    } catch (_: FileNotFoundException) {
                        null
                    }
                }

                StoredFileLocation.LEGACY_FILE -> {
                    val file = checkedLegacyFile(location)
                    if (file.isFile) FileInputStream(file) else null
                }

                else -> null
            }
        }

    override suspend fun publish(handle: ResumableFileHandle): String? =
        withContext(Dispatchers.IO) {
            when (handle.location.kind) {
                StoredFileLocation.MEDIA_STORE -> publishMediaStoreFile(handle)
                StoredFileLocation.LEGACY_FILE -> publishLegacyFile(handle)
                else -> error("Unsupported stored file location: ${handle.location.kind}")
            }
        }

    override suspend fun delete(location: StoredFileLocation) = withContext(Dispatchers.IO) {
        when (location.kind) {
            StoredFileLocation.MEDIA_STORE -> {
                val uri = ownedPendingMediaStoreUri(location) ?: return@withContext
                context.contentResolver.delete(uri, null, null)
            }

            StoredFileLocation.LEGACY_FILE -> {
                val file = checkedLegacyFile(location)
                check(!file.exists() || file.delete()) { "Unable to delete partial download" }
            }

            else -> Unit
        }
        Unit
    }

    override suspend fun complete(handle: ReceivedFileHandle): String? =
        withContext(Dispatchers.IO) {
            handle.output.flush()
            handle.output.close()
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && handle.uri != null) {
                val values = ContentValues().apply {
                    put(MediaStore.MediaColumns.IS_PENDING, 0)
                }
                check(context.contentResolver.update(handle.uri, values, null, null) > 0) {
                    "Unable to publish received file"
                }
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
    private fun createMediaStoreResumableFile(
        requestedName: String,
        mimeType: String
    ): ResumableFileHandle {
        val resolver = context.contentResolver
        val relativePath = MEDIA_STORE_RELATIVE_PATH
        val displayName = uniqueMediaStoreName(FileNamePolicy.sanitize(requestedName), relativePath)
        val values = ContentValues().apply {
            put(MediaStore.MediaColumns.DISPLAY_NAME, displayName)
            put(MediaStore.MediaColumns.MIME_TYPE, mimeType.ifBlank { "application/octet-stream" })
            put(MediaStore.MediaColumns.RELATIVE_PATH, relativePath)
            put(MediaStore.MediaColumns.IS_PENDING, 1)
        }
        val uri = resolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values)
            ?: error("Unable to create a file in Download/Transfer")
        return try {
            openMediaStoreHandle(uri, displayName)
                ?: error("Unable to open the received file")
        } catch (failure: Throwable) {
            resolver.delete(uri, null, null)
            throw failure
        }
    }

    @RequiresApi(Build.VERSION_CODES.Q)
    private fun openMediaStoreHandle(uri: Uri, displayName: String): ResumableFileHandle? {
        val descriptor = context.contentResolver.openFileDescriptor(uri, "rw") ?: return null
        return try {
            val output = FileOutputStream(descriptor.fileDescriptor)
            ChannelResumableFileHandle(
                location = StoredFileLocation(StoredFileLocation.MEDIA_STORE, uri.toString()),
                displayName = displayName,
                channel = output.channel,
                closeResources = { closeMediaStoreResources(output, descriptor) }
            )
        } catch (failure: Throwable) {
            runCatching { descriptor.close() }
            throw failure
        }
    }

    @RequiresApi(Build.VERSION_CODES.Q)
    private fun publishMediaStoreFile(handle: ResumableFileHandle): String {
        val uri = checkNotNull(ownedPendingMediaStoreUri(handle.location)) {
            "Pending MediaStore download no longer exists"
        }
        handle.close()
        val values = ContentValues().apply { put(MediaStore.MediaColumns.IS_PENDING, 0) }
        check(context.contentResolver.update(uri, values, null, null) > 0) {
            "Unable to publish received file"
        }
        return uri.toString()
    }

    private fun closeMediaStoreResources(
        output: FileOutputStream,
        descriptor: ParcelFileDescriptor
    ) {
        val outputFailure = runCatching { output.close() }.exceptionOrNull()
        val descriptorFailure = runCatching { descriptor.close() }.exceptionOrNull()
        when {
            outputFailure != null -> {
                descriptorFailure?.let(outputFailure::addSuppressed)
                throw outputFailure
            }

            descriptorFailure != null -> throw descriptorFailure
        }
    }

    private fun createLegacyResumableFile(
        transferId: String,
        requestedName: String
    ): ResumableFileHandle {
        val directory = legacyDirectory()
        val safeTransferId = FileNamePolicy.sanitize(transferId)
        val partialFile = LegacyDownloadFiles.requireContained(
            directory,
            File(directory, ".transfer-$safeTransferId.part").path
        )
        val randomAccessFile = RandomAccessFile(partialFile, "rw")
        return try {
            randomAccessFile.setLength(0)
            legacyHandle(partialFile, FileNamePolicy.sanitize(requestedName), randomAccessFile)
        } catch (failure: Throwable) {
            runCatching { randomAccessFile.close() }
            throw failure
        }
    }

    private fun reopenLegacyFile(
        location: StoredFileLocation,
        displayName: String
    ): ResumableFileHandle? {
        val file = checkedLegacyFile(location)
        if (!file.isFile) return null
        val randomAccessFile = RandomAccessFile(file, "rw")
        return legacyHandle(file, FileNamePolicy.sanitize(displayName), randomAccessFile)
    }

    private fun legacyHandle(
        file: File,
        displayName: String,
        randomAccessFile: RandomAccessFile
    ): ResumableFileHandle = ChannelResumableFileHandle(
        location = StoredFileLocation(
            StoredFileLocation.LEGACY_FILE,
            file.canonicalPath
        ),
        displayName = displayName,
        channel = randomAccessFile.channel,
        closeResources = randomAccessFile::close
    )

    private fun publishLegacyFile(handle: ResumableFileHandle): String {
        val partialFile = checkedLegacyFile(handle.location)
        val finalName = FileNamePolicy.sanitize(handle.displayName)
        handle.close()
        val reservedFile = LegacyDownloadFiles.reserveFinalFile(
            legacyDirectory(),
            finalName,
            ::timestamp
        )
        return try {
            val publishedUri = FileProvider.getUriForFile(
                context,
                "${context.packageName}.files",
                reservedFile
            ).toString()
            check(partialFile.renameTo(reservedFile)) { "Unable to publish received file" }
            publishedUri
        } catch (failure: Throwable) {
            if (partialFile.exists() && reservedFile.exists() && !reservedFile.delete()) {
                failure.addSuppressed(IOException("Unable to remove reserved download destination"))
            }
            throw failure
        }
    }

    private fun checkedLegacyFile(location: StoredFileLocation): File {
        check(location.kind == StoredFileLocation.LEGACY_FILE) {
            "Expected a legacy file location"
        }
        return LegacyDownloadFiles.requireContained(legacyDirectory(), location.value)
    }

    @Suppress("DEPRECATION")
    private fun legacyDirectory(): File {
        check(
            ContextCompat.checkSelfPermission(context, Manifest.permission.WRITE_EXTERNAL_STORAGE) ==
                PackageManager.PERMISSION_GRANTED
        ) { "Storage permission is required to save to Download/Transfer" }
        val directory = File(
            Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS),
            "Transfer"
        ).canonicalFile
        check(directory.exists() || directory.mkdirs()) { "Unable to create Download/Transfer" }
        return directory
    }

    @RequiresApi(Build.VERSION_CODES.Q)
    private fun ownedPendingMediaStoreUri(location: StoredFileLocation): Uri? {
        val uri = mediaStoreDownloadsUri(location)
        val projection = arrayOf(
            MediaStore.MediaColumns.RELATIVE_PATH,
            MediaStore.MediaColumns.OWNER_PACKAGE_NAME,
            MediaStore.MediaColumns.IS_PENDING
        )
        val cursor = context.contentResolver.query(uri, projection, null, null, null)
            ?: return null
        cursor.use {
            if (!it.moveToFirst()) return null
            val relativePath = it.getString(0)
            val ownerPackageName = it.getString(1)
            val isPending = it.getInt(2)
            if (
                relativePath != MEDIA_STORE_RELATIVE_PATH ||
                ownerPackageName != context.packageName ||
                isPending != 1
            ) {
                throw SecurityException("MediaStore item is not an owned pending Transfer download")
            }
        }
        return uri
    }

    private fun mediaStoreDownloadsUri(location: StoredFileLocation): Uri {
        if (location.kind != StoredFileLocation.MEDIA_STORE) {
            throw SecurityException("Expected a MediaStore location")
        }
        val uri = Uri.parse(location.value)
        val pathSegments = uri.pathSegments
        if (
            uri.scheme != ContentResolver.SCHEME_CONTENT ||
            uri.authority != MediaStore.AUTHORITY ||
            pathSegments.size != 3 ||
            pathSegments[1] != "downloads" ||
            pathSegments[2].toLongOrNull() == null
        ) {
            throw SecurityException("Expected a MediaStore Downloads item URI")
        }
        return uri
    }

    @RequiresApi(Build.VERSION_CODES.Q)
    private fun createMediaStoreFile(requestedName: String, mimeType: String): ReceivedFileHandle {
        val resolver = context.contentResolver
        val relativePath = MEDIA_STORE_RELATIVE_PATH
        val displayName = uniqueMediaStoreName(FileNamePolicy.sanitize(requestedName), relativePath)
        val values = ContentValues().apply {
            put(MediaStore.MediaColumns.DISPLAY_NAME, displayName)
            put(MediaStore.MediaColumns.MIME_TYPE, mimeType.ifBlank { "application/octet-stream" })
            put(MediaStore.MediaColumns.RELATIVE_PATH, relativePath)
            put(MediaStore.MediaColumns.IS_PENDING, 1)
        }
        val uri = resolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values)
            ?: error("Unable to create a file in Download/Transfer")
        val output = resolver.openOutputStream(uri, "w") ?: run {
            resolver.delete(uri, null, null)
            error("Unable to open the received file")
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
        val selection =
            "${MediaStore.MediaColumns.DISPLAY_NAME}=? AND ${MediaStore.MediaColumns.RELATIVE_PATH}=?"
        context.contentResolver.query(
            MediaStore.Downloads.EXTERNAL_CONTENT_URI,
            projection,
            selection,
            arrayOf(name, relativePath),
            null
        )?.use { return it.moveToFirst() }
        return false
    }

    @Suppress("DEPRECATION")
    private fun createLegacyFile(requestedName: String): ReceivedFileHandle {
        val file = LegacyDownloadFiles.reserveFinalFile(
            legacyDirectory(),
            requestedName,
            ::timestamp
        )
        return ReceivedFileHandle(FileOutputStream(file), file = file, displayName = file.name)
    }

    private fun timestamp(): String =
        SimpleDateFormat("yyyyMMdd_HHmmss", Locale.US).format(Date())

    private companion object {
        val MEDIA_STORE_RELATIVE_PATH = "${Environment.DIRECTORY_DOWNLOADS}/Transfer/"
    }
}

private class ChannelResumableFileHandle(
    override val location: StoredFileLocation,
    override val displayName: String,
    private val channel: FileChannel,
    private val closeResources: () -> Unit
) : ResumableFileHandle {
    private var closed = false

    override fun length(): Long = channel.size()

    override fun writeAt(offset: Long, source: ByteArray, length: Int) {
        require(offset >= 0) { "Offset must not be negative" }
        require(length in 0..source.size) { "Invalid source length" }
        Math.addExact(offset, length.toLong())
        val buffer = ByteBuffer.wrap(source, 0, length)
        var writeOffset = offset
        while (buffer.hasRemaining()) {
            val written = channel.write(buffer, writeOffset)
            check(written > 0) { "Unable to make progress writing received file" }
            writeOffset += written
        }
    }

    override fun truncate(length: Long) {
        require(length >= 0) { "Length must not be negative" }
        channel.truncate(length)
    }

    override fun force() {
        channel.force(true)
    }

    override fun close() {
        if (closed) return
        closed = true
        closeResources()
    }
}
