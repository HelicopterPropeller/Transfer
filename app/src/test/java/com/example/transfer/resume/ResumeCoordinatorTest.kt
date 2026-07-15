package com.example.transfer.resume

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.PrefixDigestScanner
import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferOffer
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.storage.ResumableFileHandle
import com.example.transfer.storage.ResumableIncomingFileStore
import com.example.transfer.storage.StoredFileLocation
import com.example.transfer.transfer.SendFileSource
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.UUID

class ResumeCoordinatorTest {
    private val now = 1_000_000L

    @Test
    fun `stable source identity reuses transfer id`() = runBlocking {
        val store = FakeResumeStore()
        val coordinator = coordinator(store)
        val source = source(lastModified = 42L)

        val first = coordinator.prepareOutgoing(source, "peer-1", "sender-1")
        val second = coordinator.prepareOutgoing(source, "peer-1", "sender-1")

        assertEquals(first.offer.transferId, second.offer.transferId)
        assertEquals(ResumeState.NONE, second.resumeStatus.state)
        assertEquals(42L, store.outgoing.values.single().lastModified)
    }

    @Test
    fun `changed source size or last modified replaces stale transfer id`() = runBlocking {
        val store = FakeResumeStore()
        val coordinator = coordinator(store)
        val first = coordinator.prepareOutgoing(source(length = 3L, lastModified = 42L), "peer-1", "sender-1")

        val changedSize = coordinator.prepareOutgoing(
            source(bytes = byteArrayOf(1, 2, 3, 4), length = 4L, lastModified = 42L),
            "peer-1",
            "sender-1"
        )
        val changedModified = coordinator.prepareOutgoing(
            source(bytes = byteArrayOf(1, 2, 3, 4), length = 4L, lastModified = 43L),
            "peer-1",
            "sender-1"
        )

        assertNotEquals(first.offer.transferId, changedSize.offer.transferId)
        assertNotEquals(changedSize.offer.transferId, changedModified.offer.transferId)
        assertEquals(setOf(first.offer.transferId, changedSize.offer.transferId), store.deletedOutgoing.toSet())
    }

    @Test
    fun `incoming metadata mismatch is an invalid candidate`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 4L)
        store.saveIncoming(checkpoint(offer, files.put("t1", byteArrayOf(1, 2, 3, 4))).copy(fileSize = 5L))

        val status = coordinator.queryIncoming(offer)

        assertEquals(ResumeState.INVALID, status.state)
    }

    @Test
    fun `structurally corrupt checkpoint is an invalid candidate`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 4L)
        store.saveIncoming(
            checkpoint(offer, files.put("t1", byteArrayOf(1, 2, 3, 4))).copy(
                confirmedBytes = 1L,
                nextChunkIndex = 7,
                chainDigest = byteArrayOf(1)
            )
        )

        assertEquals(ResumeState.INVALID, coordinator.queryIncoming(offer).state)
    }

    @Test
    fun `resume truncates receiver bytes beyond committed prefix`() = runBlocking {
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE + 9) { (it % 251).toByte() }
        val confirmed = TransferProtocol.CHUNK_SIZE.toLong()
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = bytes.size.toLong())
        val location = files.put(offer.transferId, bytes)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(bytes), confirmed)
        store.saveIncoming(checkpoint(offer, location, prefix))
        val expected = coordinator.queryIncoming(offer)

        val session = coordinator.openIncoming(offer, TransferStartMode.RESUME, expected)

        assertEquals(confirmed, session.handle.length())
        assertEquals(confirmed, session.prefix.scannedBytes)
        assertTrue(coordinator.validatePrefix(expected, session.prefix))
    }

    @Test
    fun `resume rejects receiver shorter than committed prefix`() = runBlocking {
        val committedBytes = ByteArray(TransferProtocol.CHUNK_SIZE) { 7 }
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = committedBytes.size.toLong() + 1)
        val location = files.put(offer.transferId, committedBytes.copyOf(committedBytes.size - 1))
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(committedBytes), committedBytes.size.toLong())
        store.saveIncoming(checkpoint(offer, location, prefix))

        assertThrows<ResumeValidationException> {
            coordinator.openIncoming(offer, TransferStartMode.RESUME, coordinator.queryIncoming(offer))
        }
        Unit
    }

    @Test
    fun `sender or receiver prefix chain mismatch is invalid`() = runBlocking {
        val receiver = PrefixDigestScanner.scan(ByteArrayInputStream(byteArrayOf(1, 2, 3)), 3, 3)
        val sender = PrefixDigestScanner.scan(ByteArrayInputStream(byteArrayOf(1, 2, 4)), 3, 3)
        val status = ResumeStatus(
            state = ResumeState.AVAILABLE,
            confirmedBytes = receiver.scannedBytes,
            nextChunkIndex = receiver.nextChunkIndex,
            chainDigest = receiver.chainDigest,
            lastChunkHash = receiver.lastChunkHash
        )

        assertFalse(coordinator().validatePrefix(status, sender))
        assertTrue(coordinator().validatePrefix(status, receiver))
    }

    @Test
    fun `resume rejects corrupt receiver prefix`() = runBlocking {
        val original = ByteArray(TransferProtocol.CHUNK_SIZE) { 3 }
        val corrupt = original.copyOf().also { it[0] = 4 }
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = original.size.toLong() + 1)
        val location = files.put(offer.transferId, corrupt)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(original), original.size.toLong())
        store.saveIncoming(checkpoint(offer, location, prefix))

        assertThrows<ResumeValidationException> {
            coordinator.openIncoming(offer, TransferStartMode.RESUME, coordinator.queryIncoming(offer))
        }
        Unit
    }

    @Test
    fun `restart leases then deletes old partial and creates zero checkpoint`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 9L)
        val old = files.put(offer.transferId, byteArrayOf(1, 2, 3))
        store.saveIncoming(checkpoint(offer, old))
        val expected = coordinator.queryIncoming(offer)

        val session = coordinator.openIncoming(offer, TransferStartMode.RESTART, expected)

        assertEquals(listOf(old), files.deleted)
        assertEquals(0L, session.checkpoint.confirmedBytes)
        assertEquals(0, session.checkpoint.nextChunkIndex)
        assertArrayEquals(ByteArray(ChunkCodec.DIGEST_SIZE), session.checkpoint.chainDigest)
        assertTrue(store.expiryUpdates.isNotEmpty())
    }

    @Test
    fun `claimed checkpoint fails closed before restart deletes partial`() = runBlocking {
        val store = FakeResumeStore().apply { allowExpiryUpdate = false }
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer))
        }
        assertTrue(files.deleted.isEmpty())
    }

    @Test
    fun `checkpoint deleted during lease refresh fails with conflict`() = runBlocking {
        val store = FakeResumeStore().apply { removeAfterExpiryUpdate = true }
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer))
        }
        assertTrue(files.deleted.isEmpty())
    }

    @Test
    fun `checkpoint advanced during lease refresh fails before resume touches partial`() = runBlocking {
        val store = FakeResumeStore().apply { advanceAfterExpiryUpdate = true }
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 1L)
        val location = files.put(offer.transferId, ByteArray(0))
        store.saveIncoming(checkpoint(offer, location))

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESUME, coordinator.queryIncoming(offer))
        }
        assertTrue(files.deleted.isEmpty())
        assertEquals(0, files.reopenCount)
    }

    @Test
    fun `commit forces bytes before conditional checkpoint advance`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 2L)
        val session = coordinator.openIncoming(offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        session.handle.writeAt(0, byteArrayOf(8), 1)
        store.forceObserved = { (session.handle as FakeHandle).forced }

        val committed = coordinator.commitChunk(
            session = session,
            confirmedBytes = 1L,
            nextChunkIndex = 1,
            chainDigest = ByteArray(ChunkCodec.DIGEST_SIZE) { 1 },
            lastChunkHash = ByteArray(ChunkCodec.DIGEST_SIZE) { 2 }
        )

        assertTrue((session.handle as FakeHandle).forced)
        assertEquals(now + ResumeCoordinator.RETENTION_MILLIS, committed.checkpoint.expiresAt)
        assertEquals(1L, committed.checkpoint.confirmedBytes)
    }

    @Test
    fun `stale commit index throws checkpoint conflict`() = runBlocking {
        val store = FakeResumeStore().apply { allowCommit = false }
        val coordinator = coordinator(store, FakeFiles())
        val session = coordinator.openIncoming(offer(), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))

        assertThrows<CheckpointConflictException> {
            coordinator.commitChunk(
                session,
                1L,
                1,
                ByteArray(ChunkCodec.DIGEST_SIZE),
                ByteArray(ChunkCodec.DIGEST_SIZE)
            )
        }
        Unit
    }

    @Test
    fun `success deletes only the relevant local checkpoint or link`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val prepared = coordinator.prepareOutgoing(source(), "peer-1", "sender-1")
        val incoming = coordinator.openIncoming(offer(), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))

        assertEquals("fake://${incoming.handle.location.value}", coordinator.completeIncoming(incoming))
        assertNull(store.findIncoming(incoming.checkpoint.transferId))
        assertTrue(store.outgoing.isNotEmpty())

        coordinator.completeOutgoing(prepared.offer.transferId)
        assertTrue(store.outgoing.isEmpty())
    }

    private fun coordinator(
        store: FakeResumeStore = FakeResumeStore(),
        files: FakeFiles = FakeFiles()
    ) = ResumeCoordinator(store, files, clock = { now }, newTransferId = { UUID.randomUUID().toString() })

    private fun source(
        bytes: ByteArray = byteArrayOf(1, 2, 3),
        length: Long = bytes.size.toLong(),
        lastModified: Long? = 42L
    ) = SendFileSource(
        displayName = "a.bin",
        mimeType = "application/octet-stream",
        length = length,
        sourceUri = "content://a",
        lastModified = lastModified,
        openStream = { ByteArrayInputStream(bytes) }
    )

    private fun offer(fileSize: Long = 3L) = TransferOffer(
        transferId = "11111111-1111-1111-1111-111111111111",
        senderDeviceId = "sender-1",
        fileName = "a.bin",
        mimeType = "application/octet-stream",
        fileSize = fileSize
    )

    private fun checkpoint(
        offer: TransferOffer,
        location: StoredFileLocation,
        prefix: com.example.transfer.protocol.PrefixDigest = PrefixDigestScanner.scan(
            ByteArrayInputStream(ByteArray(0)),
            0
        )
    ) = IncomingCheckpoint(
        transferId = offer.transferId,
        senderDeviceId = offer.senderDeviceId,
        fileName = offer.fileName,
        displayName = offer.fileName,
        mimeType = offer.mimeType,
        fileSize = offer.fileSize,
        chunkSize = offer.chunkSize,
        confirmedBytes = prefix.scannedBytes,
        nextChunkIndex = prefix.nextChunkIndex,
        chainDigest = prefix.chainDigest,
        lastChunkHash = prefix.lastChunkHash,
        storageKind = location.kind,
        storageValue = location.value,
        createdAt = now - 1,
        updatedAt = now - 1,
        expiresAt = now + ResumeCoordinator.RETENTION_MILLIS
    )
}

internal inline fun <reified T : Throwable> assertThrows(block: () -> Unit): T {
    try {
        block()
    } catch (error: Throwable) {
        if (error is T) return error
        throw error
    }
    fail("Expected ${T::class.java.simpleName}")
    throw AssertionError("unreachable")
}

private class FakeResumeStore : ResumeStore {
    val incoming = linkedMapOf<String, IncomingCheckpoint>()
    val outgoing = linkedMapOf<String, OutgoingResumeLink>()
    val deletedOutgoing = mutableListOf<String>()
    val expiryUpdates = mutableListOf<String>()
    var allowCommit = true
    var allowExpiryUpdate = true
    var removeAfterExpiryUpdate = false
    var advanceAfterExpiryUpdate = false
    var forceObserved: (() -> Boolean)? = null

    override suspend fun findIncoming(transferId: String) = incoming[transferId]
    override suspend fun saveIncoming(checkpoint: IncomingCheckpoint) { incoming[checkpoint.transferId] = checkpoint }
    override suspend fun commitIncomingChunk(
        transferId: String, expectedNextChunkIndex: Int, confirmedBytes: Long,
        nextChunkIndex: Int, chainDigest: ByteArray, lastChunkHash: ByteArray,
        updatedAt: Long, expiresAt: Long
    ): Boolean {
        check(forceObserved?.invoke() != false) { "commit happened before force" }
        val current = incoming[transferId]
        if (!allowCommit || current == null || current.nextChunkIndex != expectedNextChunkIndex) return false
        incoming[transferId] = current.copy(
            confirmedBytes = confirmedBytes, nextChunkIndex = nextChunkIndex,
            chainDigest = chainDigest, lastChunkHash = lastChunkHash,
            updatedAt = updatedAt, expiresAt = expiresAt
        )
        return true
    }
    override suspend fun updateIncomingExpiry(transferId: String, updatedAt: Long, expiresAt: Long): Boolean {
        expiryUpdates += transferId
        val current = incoming[transferId]
        if (!allowExpiryUpdate || current == null) return false
        incoming[transferId] = current.copy(updatedAt = updatedAt, expiresAt = expiresAt)
        if (removeAfterExpiryUpdate) incoming.remove(transferId)
        if (advanceAfterExpiryUpdate) {
            incoming[transferId] = incoming.getValue(transferId).copy(
                confirmedBytes = incoming.getValue(transferId).fileSize,
                nextChunkIndex = 1,
                chainDigest = ByteArray(ChunkCodec.DIGEST_SIZE) { 3 },
                lastChunkHash = ByteArray(ChunkCodec.DIGEST_SIZE) { 4 }
            )
        }
        return true
    }
    override suspend fun deleteIncoming(transferId: String) { incoming.remove(transferId) }
    override suspend fun findOutgoing(sourceUri: String, peerDeviceId: String) =
        outgoing.values.singleOrNull { it.sourceUri == sourceUri && it.peerDeviceId == peerDeviceId }
    override suspend fun saveOutgoing(link: OutgoingResumeLink) { outgoing[link.transferId] = link }
    override suspend fun updateOutgoingTimestamp(transferId: String, updatedAt: Long): Boolean {
        val link = outgoing[transferId] ?: return false
        outgoing[transferId] = link.copy(updatedAt = updatedAt)
        return true
    }
    override suspend fun deleteOutgoing(transferId: String) { deletedOutgoing += transferId; outgoing.remove(transferId) }
    override suspend fun deleteCompleted(transferId: String) { incoming.remove(transferId); outgoing.remove(transferId) }
    override suspend fun claimExpiredIncoming(now: Long, staleClaimBefore: Long, token: String) = emptyList<IncomingCheckpoint>()
    override suspend fun deleteClaimedIncoming(token: String) = 0
    override suspend fun releaseClaimedIncoming(token: String) = 0
    override suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long) = 0
}

private class FakeFiles : ResumableIncomingFileStore {
    private val entries = linkedMapOf<StoredFileLocation, FakeEntry>()
    val deleted = mutableListOf<StoredFileLocation>()
    var reopenCount = 0

    fun put(id: String, bytes: ByteArray): StoredFileLocation {
        val location = StoredFileLocation("FAKE", id)
        entries[location] = FakeEntry("a.bin", bytes)
        return location
    }

    override suspend fun create(transferId: String, fileName: String, mimeType: String): ResumableFileHandle {
        val location = StoredFileLocation("FAKE", "$transferId-${entries.size}")
        val entry = FakeEntry(fileName, ByteArray(0))
        entries[location] = entry
        return FakeHandle(location, entry)
    }
    override suspend fun reopen(location: StoredFileLocation, displayName: String): ResumableFileHandle? {
        reopenCount++
        return entries[location]?.let { FakeHandle(location, it) }
    }
    override suspend fun openInput(location: StoredFileLocation): InputStream? =
        entries[location]?.let { ByteArrayInputStream(it.bytes) }
    override suspend fun publish(handle: ResumableFileHandle): String {
        handle.close()
        return "fake://${handle.location.value}"
    }
    override suspend fun delete(location: StoredFileLocation) { deleted += location; entries.remove(location) }
}

private data class FakeEntry(val name: String, var bytes: ByteArray)

private class FakeHandle(
    override val location: StoredFileLocation,
    private val entry: FakeEntry
) : ResumableFileHandle {
    override val displayName: String get() = entry.name
    var forced = false
    private var closed = false
    override fun length() = entry.bytes.size.toLong()
    override fun writeAt(offset: Long, source: ByteArray, length: Int) {
        val end = offset.toInt() + length
        if (end > entry.bytes.size) entry.bytes = entry.bytes.copyOf(end)
        source.copyInto(entry.bytes, offset.toInt(), 0, length)
    }
    override fun truncate(length: Long) { entry.bytes = entry.bytes.copyOf(length.toInt()) }
    override fun force() { forced = true }
    override fun close() { closed = true }
}
