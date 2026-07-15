package com.example.transfer.resume

import com.example.transfer.storage.ResumableFileHandle
import com.example.transfer.storage.ResumableIncomingFileStore
import com.example.transfer.storage.StoredFileLocation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CancellationException
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.InputStream

class ResumeCleanupTest {
    private val now = 10 * ResumeCleanup.DAY_MILLIS

    @Test
    fun `cleanup claims seven day boundary and outgoing cutoff inclusively`() = runBlocking {
        val store = CleanupStore(listOf(checkpoint("expired")))
        val files = CleanupFiles()
        var savedLastRun = 0L
        val cleanup = ResumeCleanup(store, files, { now }, { 0L }, { savedLastRun = it })

        assertEquals(1, cleanup.runIfDue(force = true))

        assertEquals(now, store.claimNow)
        assertEquals(now - ResumeCleanup.DAY_MILLIS, store.staleClaimBefore)
        assertEquals(now - ResumeCleanup.RETENTION_MILLIS, store.outgoingCutoff)
        assertEquals(listOf(StoredFileLocation("FAKE", "expired")), files.deleted)
        assertTrue(store.deletedClaimToken != null)
        assertEquals(now, savedLastRun)
    }

    @Test
    fun `cleanup is throttled until a full day has elapsed`() = runBlocking {
        val store = CleanupStore(listOf(checkpoint("expired")))
        val cleanup = ResumeCleanup(
            store,
            CleanupFiles(),
            { now },
            { now - ResumeCleanup.DAY_MILLIS + 1 },
            {}
        )

        assertEquals(0, cleanup.runIfDue())
        assertNullValue(store.claimNow)
    }

    @Test
    fun `file deletion failure releases claim and does not record run`() = runBlocking {
        val store = CleanupStore(listOf(checkpoint("expired")))
        val files = CleanupFiles(fail = true)
        var saved = false
        val cleanup = ResumeCleanup(store, files, { now }, { 0L }, { saved = true })

        assertThrows<IllegalStateException> { cleanup.runIfDue(force = true) }

        assertEquals(store.claimToken, store.releasedClaimToken)
        assertEquals(1, store.releaseCount)
        assertNullValue(store.deletedClaimToken)
        assertTrue(!saved)
    }

    @Test
    fun `cleanup attempts every claimed file before releasing failed claim`() = runBlocking {
        val store = CleanupStore(listOf(checkpoint("first"), checkpoint("second")))
        val files = CleanupFiles(failValues = setOf("first"))
        val cleanup = ResumeCleanup(store, files, { now }, { 0L }, {})

        assertThrows<IllegalStateException> { cleanup.runIfDue(force = true) }

        assertEquals(listOf("first", "second"), files.attempted.map { it.value })
        assertEquals(listOf("second"), files.deleted.map { it.value })
        assertEquals(store.claimToken, store.releasedClaimToken)
    }

    @Test
    fun `cleanup cancellation stops deletion releases claim and rethrows`() = runBlocking {
        val store = CleanupStore(listOf(checkpoint("first"), checkpoint("second")))
        val files = CleanupFiles(cancelValue = "first")
        val cleanup = ResumeCleanup(store, files, { now }, { 0L }, {})

        assertThrows<CancellationException> { cleanup.runIfDue(force = true) }

        assertEquals(listOf("first"), files.attempted.map { it.value })
        assertEquals(store.claimToken, store.releasedClaimToken)
        assertEquals(1, store.releaseCount)
        assertNullValue(store.deletedClaimToken)
    }

    private fun checkpoint(id: String) = IncomingCheckpoint(
        transferId = id,
        senderDeviceId = "sender",
        fileName = "a.bin",
        displayName = "a.bin",
        mimeType = "application/octet-stream",
        fileSize = 1,
        chunkSize = 1_048_576,
        confirmedBytes = 0,
        nextChunkIndex = 0,
        chainDigest = ByteArray(32),
        lastChunkHash = ByteArray(32),
        storageKind = "FAKE",
        storageValue = id,
        createdAt = 0,
        updatedAt = 0,
        expiresAt = now
    )
}

private fun assertNullValue(value: Any?) = assertEquals(null, value)

private class CleanupStore(private val expired: List<IncomingCheckpoint>) : ResumeStore {
    var claimNow: Long? = null
    var staleClaimBefore: Long? = null
    var claimToken: String? = null
    var deletedClaimToken: String? = null
    var releasedClaimToken: String? = null
    var releaseCount = 0
    var outgoingCutoff: Long? = null

    override suspend fun claimExpiredIncoming(now: Long, staleClaimBefore: Long, token: String): List<IncomingCheckpoint> {
        claimNow = now
        this.staleClaimBefore = staleClaimBefore
        claimToken = token
        return expired
    }
    override suspend fun deleteClaimedIncoming(token: String): Int { deletedClaimToken = token; return expired.size }
    override suspend fun releaseClaimedIncoming(token: String): Int {
        releasedClaimToken = token
        releaseCount++
        return expired.size
    }
    override suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long): Int { outgoingCutoff = updatedAtCutoff; return 1 }
    override suspend fun findIncoming(transferId: String) = null
    override suspend fun saveIncoming(checkpoint: IncomingCheckpoint) = Unit
    override suspend fun commitIncomingChunk(transferId: String, expectedNextChunkIndex: Int, confirmedBytes: Long, nextChunkIndex: Int, chainDigest: ByteArray, lastChunkHash: ByteArray, updatedAt: Long, expiresAt: Long) = false
    override suspend fun updateIncomingExpiry(transferId: String, updatedAt: Long, expiresAt: Long) = false
    override suspend fun deleteIncoming(transferId: String) = Unit
    override suspend fun findOutgoing(sourceUri: String, peerDeviceId: String) = null
    override suspend fun saveOutgoing(link: OutgoingResumeLink) = Unit
    override suspend fun updateOutgoingTimestamp(transferId: String, updatedAt: Long) = false
    override suspend fun deleteOutgoing(transferId: String) = Unit
    override suspend fun deleteCompleted(transferId: String) = Unit
}

private class CleanupFiles(
    private val fail: Boolean = false,
    private val failValues: Set<String> = emptySet(),
    private val cancelValue: String? = null
) : ResumableIncomingFileStore {
    val deleted = mutableListOf<StoredFileLocation>()
    val attempted = mutableListOf<StoredFileLocation>()
    override suspend fun delete(location: StoredFileLocation) {
        attempted += location
        if (location.value == cancelValue) throw CancellationException("cancelled")
        if (fail || location.value in failValues) throw IllegalStateException("delete failed")
        deleted += location
    }
    override suspend fun create(transferId: String, fileName: String, mimeType: String): ResumableFileHandle = error("unused")
    override suspend fun reopen(location: StoredFileLocation, displayName: String): ResumableFileHandle? = error("unused")
    override suspend fun openInput(location: StoredFileLocation): InputStream? = error("unused")
    override suspend fun publish(handle: ResumableFileHandle): String? = error("unused")
}
