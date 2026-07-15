package com.example.transfer.resume

import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class RoomResumeStoreTest {
    @Test
    fun `resume link key is source uri plus peer id`() = runBlocking {
        val dao = FakeResumeDao()
        val store = RoomResumeStore(dao)
        val link = outgoing(transferId = "t1", peerDeviceId = "peer-1")

        store.saveOutgoing(link)

        assertEquals(link, store.findOutgoing("content://a", "peer-1"))
        assertNull(store.findOutgoing("content://a", "peer-2"))
    }

    @Test
    fun `completed transfer deletes incoming checkpoint and outgoing link`() = runBlocking {
        val dao = FakeResumeDao()
        val store = RoomResumeStore(dao)
        store.saveIncoming(incoming("t1"))
        store.saveOutgoing(outgoing("t1"))

        store.deleteCompleted("t1")

        assertNull(store.findIncoming("t1"))
        assertNull(store.findOutgoing("content://a", "peer-1"))
    }

    @Test
    fun `interruption updates incoming expiry`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        store.saveIncoming(incoming("t1"))

        assertTrue(store.updateIncomingExpiry("t1", updatedAt = 20L, expiresAt = 200L))

        val updated = store.findIncoming("t1")!!
        assertEquals(20L, updated.updatedAt)
        assertEquals(200L, updated.expiresAt)
    }

    @Test
    fun `commit incoming chunk rejects stale expected next chunk index`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        store.saveIncoming(incoming("t1").copy(nextChunkIndex = 2, confirmedBytes = 16L))

        val committed = store.commitIncomingChunk(
            transferId = "t1",
            expectedNextChunkIndex = 1,
            confirmedBytes = 24L,
            nextChunkIndex = 3,
            chainDigest = byteArrayOf(3),
            lastChunkHash = byteArrayOf(4),
            updatedAt = 20L,
            expiresAt = 200L
        )

        assertFalse(committed)
        val unchanged = store.findIncoming("t1")!!
        assertEquals(16L, unchanged.confirmedBytes)
        assertEquals(2, unchanged.nextChunkIndex)
        assertArrayEquals(byteArrayOf(1), unchanged.chainDigest)
        assertArrayEquals(byteArrayOf(2), unchanged.lastChunkHash)
    }

    @Test
    fun `cleanup exposes expired storage locations before deleting rows`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        store.saveIncoming(incoming("expired-media").copy(expiresAt = 90L))
        store.saveIncoming(
            incoming("expired-file").copy(
                storageKind = ResumeStorageKind.LEGACY_FILE,
                storageValue = "/tmp/a.part",
                expiresAt = 100L
            )
        )
        store.saveIncoming(incoming("fresh").copy(expiresAt = 101L))
        store.saveOutgoing(outgoing("old-outgoing").copy(updatedAt = 50L))

        val locations = store.findExpiredIncoming(now = 100L).map { it.location }

        assertEquals(
            setOf(
                ResumeStorageLocation(ResumeStorageKind.MEDIA_STORE, "content://pending/t1"),
                ResumeStorageLocation(ResumeStorageKind.LEGACY_FILE, "/tmp/a.part")
            ),
            locations.toSet()
        )
        assertEquals(3, store.deleteExpired(now = 100L, outgoingUpdatedAtCutoff = 50L))
        assertNull(store.findIncoming("expired-media"))
        assertNull(store.findIncoming("expired-file"))
        assertEquals("fresh", store.findIncoming("fresh")?.transferId)
        assertNull(store.findOutgoing("content://a", "peer-1"))
    }

    private fun incoming(transferId: String) = IncomingCheckpoint(
        transferId = transferId,
        senderDeviceId = "sender-1",
        fileName = "a.bin",
        displayName = "a.bin",
        mimeType = "application/octet-stream",
        fileSize = 32L,
        chunkSize = 8,
        confirmedBytes = 8L,
        nextChunkIndex = 1,
        chainDigest = byteArrayOf(1),
        lastChunkHash = byteArrayOf(2),
        storageKind = ResumeStorageKind.MEDIA_STORE,
        storageValue = "content://pending/t1",
        createdAt = 10L,
        updatedAt = 10L,
        expiresAt = 100L
    )

    private fun outgoing(
        transferId: String,
        peerDeviceId: String = "peer-1"
    ) = OutgoingResumeLink(
        transferId = transferId,
        sourceUri = "content://a",
        peerDeviceId = peerDeviceId,
        fileName = "a.bin",
        mimeType = "application/octet-stream",
        fileSize = 8L,
        lastModified = 10L,
        chunkSize = 1,
        createdAt = 1L,
        updatedAt = 1L
    )
}

private class FakeResumeDao : ResumeDao {
    private val incoming = linkedMapOf<String, IncomingCheckpointEntity>()
    private val outgoing = linkedMapOf<String, OutgoingResumeLinkEntity>()

    override suspend fun findIncoming(transferId: String): IncomingCheckpointEntity? =
        incoming[transferId]

    override suspend fun upsertIncoming(entity: IncomingCheckpointEntity) {
        incoming[entity.transferId] = entity
    }

    override suspend fun commitIncomingChunk(
        transferId: String,
        expectedNextChunkIndex: Int,
        confirmedBytes: Long,
        nextChunkIndex: Int,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray,
        updatedAt: Long,
        expiresAt: Long
    ): Int {
        val current = incoming[transferId]
        if (current == null || current.nextChunkIndex != expectedNextChunkIndex) return 0
        incoming[transferId] = current.copy(
            confirmedBytes = confirmedBytes,
            nextChunkIndex = nextChunkIndex,
            chainDigest = chainDigest,
            lastChunkHash = lastChunkHash,
            updatedAt = updatedAt,
            expiresAt = expiresAt
        )
        return 1
    }

    override suspend fun updateIncomingExpiry(
        transferId: String,
        updatedAt: Long,
        expiresAt: Long
    ): Int {
        val current = incoming[transferId] ?: return 0
        incoming[transferId] = current.copy(updatedAt = updatedAt, expiresAt = expiresAt)
        return 1
    }

    override suspend fun deleteIncoming(transferId: String): Int =
        if (incoming.remove(transferId) != null) 1 else 0

    override suspend fun findOutgoing(
        sourceUri: String,
        peerDeviceId: String
    ): OutgoingResumeLinkEntity? = outgoing.values.singleOrNull {
        it.sourceUri == sourceUri && it.peerDeviceId == peerDeviceId
    }

    override suspend fun upsertOutgoing(entity: OutgoingResumeLinkEntity) {
        outgoing.entries.removeAll {
            it.value.sourceUri == entity.sourceUri && it.value.peerDeviceId == entity.peerDeviceId
        }
        outgoing[entity.transferId] = entity
    }

    override suspend fun updateOutgoingTimestamp(transferId: String, updatedAt: Long): Int {
        val current = outgoing[transferId] ?: return 0
        outgoing[transferId] = current.copy(updatedAt = updatedAt)
        return 1
    }

    override suspend fun deleteOutgoing(transferId: String): Int =
        if (outgoing.remove(transferId) != null) 1 else 0

    override suspend fun deleteCompleted(transferId: String) {
        incoming.remove(transferId)
        outgoing.remove(transferId)
    }

    override suspend fun findExpiredIncoming(now: Long): List<IncomingCheckpointEntity> =
        incoming.values.filter { it.expiresAt <= now }

    override suspend fun deleteExpiredIncoming(now: Long): Int {
        val ids = incoming.values.filter { it.expiresAt <= now }.map { it.transferId }
        ids.forEach(incoming::remove)
        return ids.size
    }

    override suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long): Int {
        val ids = outgoing.values.filter { it.updatedAt <= updatedAtCutoff }.map { it.transferId }
        ids.forEach(outgoing::remove)
        return ids.size
    }

    override suspend fun deleteExpired(now: Long, outgoingUpdatedAtCutoff: Long): Int {
        val incomingIds = incoming.values.filter { it.expiresAt <= now }.map { it.transferId }
        val outgoingIds = outgoing.values.filter { it.updatedAt <= outgoingUpdatedAtCutoff }.map { it.transferId }
        incomingIds.forEach(incoming::remove)
        outgoingIds.forEach(outgoing::remove)
        return incomingIds.size + outgoingIds.size
    }
}
