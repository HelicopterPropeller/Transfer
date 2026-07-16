package com.example.transfer.storage

import android.content.ContentValues
import android.net.Uri
import android.os.Environment
import android.provider.MediaStore
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.SdkSuppress
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
@SdkSuppress(minSdkVersion = 29)
class DownloadStorageInstrumentedTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private val resolver = context.contentResolver
    private val createdUris = mutableListOf<Uri>()

    @After
    fun cleanUp() {
        createdUris.forEach { runCatching { resolver.delete(it, null, null) } }
    }

    @Test
    fun createReopenWriteReadAndDeleteReturnsNullableMissingItem() = runBlocking {
        val storage = DownloadStorage(context)
        val stagingId = "instrumented-reopen-${System.nanoTime()}"
        val created = storage.create(
            transferId = stagingId,
            fileName = "resume.bin",
            mimeType = "application/octet-stream"
        )
        createdUris += Uri.parse(created.location.value)
        assertEquals(created.location, storage.findStaging(stagingId))
        created.writeAt(0, byteArrayOf(1, 2, 3, 4), 4)
        created.force()
        created.close()

        val reopened = storage.reopen(created.location, "resume.bin")
        assertNotNull(reopened)
        requireNotNull(reopened).writeAt(2, byteArrayOf(8, 9), 2)
        reopened.force()
        assertArrayEquals(byteArrayOf(1, 2, 8, 9), storage.openInput(created.location)?.readBytes())
        assertArrayEquals(
            byteArrayOf(1, 2, 8, 9),
            storage.openCompletionInput(created.location, "resume.bin")?.readBytes()
        )
        reopened.close()

        storage.delete(created.location)
        storage.delete(created.location)
        assertNull(storage.reopen(created.location, "resume.bin"))
        assertNull(storage.openInput(created.location))
    }

    @Test
    fun publishClearsPendingFlag() = runBlocking {
        val storage = DownloadStorage(context)
        val created = storage.create(
            transferId = "instrumented-publish-${System.nanoTime()}",
            fileName = "publish.bin",
            mimeType = "application/octet-stream"
        )
        val uri = Uri.parse(created.location.value)
        createdUris += uri
        created.writeAt(0, byteArrayOf(4, 5, 6), 3)
        created.force()

        assertEquals(uri.toString(), storage.publish(created))
        assertEquals(uri.toString(), storage.recoverPublished(created.location, "publish.bin"))
        assertArrayEquals(
            byteArrayOf(4, 5, 6),
            storage.openCompletionInput(created.location, "publish.bin")?.readBytes()
        )
        assertThrows(SecurityException::class.java) {
            runBlocking { storage.delete(created.location) }
        }

        resolver.query(
            uri,
            arrayOf(MediaStore.MediaColumns.IS_PENDING),
            null,
            null,
            null
        )?.use { cursor ->
            assertTrue(cursor.moveToFirst())
            assertEquals(0, cursor.getInt(0))
        } ?: error("Published MediaStore row disappeared")
    }

    @Test
    fun sameOwnerPendingItemOutsideTransferIsRejectedAndNotDeleted() = runBlocking {
        val values = ContentValues().apply {
            put(MediaStore.MediaColumns.DISPLAY_NAME, "outside-${System.nanoTime()}.bin")
            put(MediaStore.MediaColumns.MIME_TYPE, "application/octet-stream")
            put(MediaStore.MediaColumns.RELATIVE_PATH, "${Environment.DIRECTORY_DOWNLOADS}/OutsideTransfer")
            put(MediaStore.MediaColumns.IS_PENDING, 1)
        }
        val uri = requireNotNull(
            resolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values)
        )
        createdUris += uri
        val location = StoredFileLocation(StoredFileLocation.MEDIA_STORE, uri.toString())
        val storage = DownloadStorage(context)

        assertThrows(SecurityException::class.java) {
            runBlocking { storage.reopen(location, "outside.bin") }
        }
        assertThrows(SecurityException::class.java) {
            runBlocking { storage.delete(location) }
        }

        resolver.query(uri, arrayOf(MediaStore.MediaColumns._ID), null, null, null)?.use {
            assertTrue(it.moveToFirst())
        } ?: error("Rejected MediaStore row was deleted")
    }
}
