package com.example.transfer.storage

import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.InputStream

class ResumableFileStoreContractTest {
    @Test
    fun `file can be reopened read truncated published and deleted idempotently`() = runBlocking {
        val store = FakeResumableIncomingFileStore()
        val created = store.create("transfer-1", "report.bin", "application/octet-stream")
        val location = created.location

        created.writeAt(0, byteArrayOf(1, 2, 3, 4), 4)
        created.writeAt(2, byteArrayOf(8, 9, 10), 2)
        created.force()
        assertEquals(4, created.length())
        created.close()

        val reopened = store.reopen(location, "report.bin")
        assertNotNull(reopened)
        assertArrayEquals(byteArrayOf(1, 2, 8, 9), store.openInput(location)?.readBytes())
        requireNotNull(reopened).truncate(3)
        reopened.force()
        assertEquals("fake://transfer-1", store.publish(reopened))
        assertArrayEquals(byteArrayOf(1, 2, 8), store.openInput(location)?.readBytes())

        store.delete(location)
        store.delete(location)
        assertNull(store.reopen(location, "report.bin"))
        assertNull(store.openInput(location))
    }

    private class FakeResumableIncomingFileStore : ResumableIncomingFileStore {
        private val files = mutableMapOf<StoredFileLocation, Entry>()

        override suspend fun create(
            transferId: String,
            fileName: String,
            mimeType: String
        ): ResumableFileHandle {
            val location = StoredFileLocation("FAKE", transferId)
            val entry = Entry(fileName)
            files[location] = entry
            return FakeHandle(location, entry)
        }

        override suspend fun reopen(
            location: StoredFileLocation,
            displayName: String
        ): ResumableFileHandle? = files[location]?.let {
            FakeHandle(location, it, displayName)
        }

        override suspend fun openInput(location: StoredFileLocation): InputStream? =
            files[location]?.let { ByteArrayInputStream(it.bytes) }

        override suspend fun publish(
            handle: ResumableFileHandle,
            expectedDigest: ByteArray?
        ): String {
            handle.close()
            return "fake://${handle.location.value}"
        }

        override suspend fun delete(location: StoredFileLocation) {
            files.remove(location)
        }
    }

    private data class Entry(
        val displayName: String,
        var bytes: ByteArray = ByteArray(0)
    )

    private class FakeHandle(
        override val location: StoredFileLocation,
        private val entry: Entry,
        override val displayName: String = entry.displayName
    ) : ResumableFileHandle {
        private var closed = false

        override fun length(): Long {
            checkOpen()
            return entry.bytes.size.toLong()
        }

        override fun writeAt(offset: Long, source: ByteArray, length: Int) {
            checkOpen()
            require(offset >= 0)
            require(length in 0..source.size)
            val end = Math.addExact(offset, length.toLong())
            require(end <= Int.MAX_VALUE)
            if (end > entry.bytes.size) entry.bytes = entry.bytes.copyOf(end.toInt())
            source.copyInto(entry.bytes, offset.toInt(), 0, length)
        }

        override fun truncate(length: Long) {
            checkOpen()
            require(length in 0..Int.MAX_VALUE.toLong())
            entry.bytes = entry.bytes.copyOf(length.toInt())
        }

        override fun force() {
            checkOpen()
        }

        override fun close() {
            closed = true
        }

        private fun checkOpen() = check(!closed) { "Handle is closed" }
    }
}
