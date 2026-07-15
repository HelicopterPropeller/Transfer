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
    fun `cleanup claim atomically excludes a competing worker`() = runBlocking {
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

        val firstClaim = store.claimExpiredIncoming(
            now = 100L,
            staleClaimBefore = 80L,
            token = "claim-1"
        )
        val competingClaim = store.claimExpiredIncoming(
            now = 100L,
            staleClaimBefore = 80L,
            token = "claim-2"
        )

        assertEquals(
            setOf(
                ResumeStorageLocation(ResumeStorageKind.MEDIA_STORE, "content://pending/t1"),
                ResumeStorageLocation(ResumeStorageKind.LEGACY_FILE, "/tmp/a.part")
            ),
            firstClaim.map { it.location }.toSet()
        )
        assertTrue(competingClaim.isEmpty())
        assertEquals("fresh", store.findIncoming("fresh")?.transferId)
    }

    @Test
    fun `stale cleanup claim can be reclaimed and released`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        store.saveIncoming(incoming("t1").copy(expiresAt = 40L))
        assertEquals(
            listOf("t1"),
            store.claimExpiredIncoming(50L, staleClaimBefore = 0L, token = "claim-1")
                .map { it.transferId }
        )

        assertEquals(
            listOf("t1"),
            store.claimExpiredIncoming(100L, staleClaimBefore = 51L, token = "claim-2")
                .map { it.transferId }
        )
        assertEquals(1, store.releaseClaimedIncoming("claim-2"))
        assertEquals(
            listOf("t1"),
            store.claimExpiredIncoming(100L, staleClaimBefore = 90L, token = "claim-3")
                .map { it.transferId }
        )
    }

    @Test
    fun `claimed checkpoint rejects progress and expiry updates`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        store.saveIncoming(incoming("t1").copy(expiresAt = 40L))
        store.claimExpiredIncoming(50L, staleClaimBefore = 0L, token = "claim-1")

        assertFalse(
            store.commitIncomingChunk(
                transferId = "t1",
                expectedNextChunkIndex = 1,
                confirmedBytes = 16L,
                nextChunkIndex = 2,
                chainDigest = byteArrayOf(3),
                lastChunkHash = byteArrayOf(4),
                updatedAt = 60L,
                expiresAt = 200L
            )
        )
        assertFalse(store.updateIncomingExpiry("t1", updatedAt = 60L, expiresAt = 200L))
        assertEquals(8L, store.findIncoming("t1")?.confirmedBytes)
        assertEquals(40L, store.findIncoming("t1")?.expiresAt)
    }

    @Test
    fun `cleanup mutates only rows owned by its token`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        store.saveIncoming(incoming("claim-1-row").copy(expiresAt = 40L))
        store.claimExpiredIncoming(50L, staleClaimBefore = 0L, token = "claim-1")
        store.saveIncoming(incoming("claim-2-row").copy(expiresAt = 40L))
        store.claimExpiredIncoming(50L, staleClaimBefore = 0L, token = "claim-2")
        store.saveOutgoing(outgoing("old-outgoing").copy(updatedAt = 50L))

        assertEquals(1, store.deleteClaimedIncoming("claim-1"))
        assertNull(store.findIncoming("claim-1-row"))
        assertEquals("claim-2-row", store.findIncoming("claim-2-row")?.transferId)
        assertEquals(1, store.deleteExpiredOutgoing(updatedAtCutoff = 50L))
        assertNull(store.findOutgoing("content://a", "peer-1"))
    }

    @Test
    fun `session acquisition has one winner and stale startup claim can be cleared`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        val row = incoming("t1")
        store.saveIncoming(row)

        val winner = store.acquireIncomingSession(row, "winner", now = 20L, expiresAt = 200L)

        assertEquals("winner", winner?.sessionToken)
        assertNull(store.acquireIncomingSession(row, "loser", now = 20L, expiresAt = 200L))
        assertEquals(0, store.clearStaleIncomingSessions(20L))
        assertEquals(1, store.clearStaleIncomingSessions(21L))
        assertNull(store.findIncoming("t1")?.sessionToken)
    }

    @Test
    fun `cleanup cannot claim an actively owned expired checkpoint`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        val row = incoming("t1").copy(expiresAt = 10L)
        store.saveIncoming(row)
        assertTrue(store.acquireIncomingSession(row, "session", now = 20L, expiresAt = 30L) != null)

        assertTrue(store.claimExpiredIncoming(30L, 0L, "cleanup").isEmpty())
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

    override suspend fun insertIncoming(entity: IncomingCheckpointEntity): Long {
        if (incoming.containsKey(entity.transferId)) return -1
        incoming[entity.transferId] = entity
        return 1
    }

    override suspend fun acquireIncomingSession(
        transferId: String, storageKind: String, storageValue: String,
        generation: Long, nextChunkIndex: Int, confirmedBytes: Long,
        chainDigest: ByteArray, lastChunkHash: ByteArray, token: String,
        now: Long, expiresAt: Long
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (current.cleanupToken != null || current.sessionToken != null ||
            current.storageKind != storageKind || current.storageValue != storageValue ||
            current.generation != generation || current.nextChunkIndex != nextChunkIndex ||
            current.confirmedBytes != confirmedBytes || !current.chainDigest.contentEquals(chainDigest) ||
            !current.lastChunkHash.contentEquals(lastChunkHash)
        ) return 0
        incoming[transferId] = current.copy(
            sessionToken = token, sessionClaimedAt = now, updatedAt = now, expiresAt = expiresAt
        )
        return 1
    }

    override suspend fun releaseIncomingSession(
        transferId: String, token: String, generation: Long, storageKind: String,
        storageValue: String, nextChunkIndex: Int
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, generation, storageKind, storageValue, nextChunkIndex)) return 0
        incoming[transferId] = current.copy(sessionToken = null, sessionClaimedAt = null)
        return 1
    }

    override suspend fun clearStaleIncomingSessions(staleClaimBefore: Long): Int {
        val rows = incoming.values.filter {
            it.sessionToken != null && (it.sessionClaimedAt ?: Long.MAX_VALUE) < staleClaimBefore
        }
        rows.forEach { incoming[it.transferId] = it.copy(sessionToken = null, sessionClaimedAt = null) }
        return rows.size
    }

    override suspend fun commitOwnedIncomingChunk(
        transferId: String, token: String, generation: Long, storageKind: String,
        storageValue: String, expectedNextChunkIndex: Int, confirmedBytes: Long,
        nextChunkIndex: Int, chainDigest: ByteArray, lastChunkHash: ByteArray,
        updatedAt: Long, expiresAt: Long
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, generation, storageKind, storageValue, expectedNextChunkIndex) ||
            current.cleanupToken != null
        ) return 0
        incoming[transferId] = current.copy(
            confirmedBytes = confirmedBytes, nextChunkIndex = nextChunkIndex,
            chainDigest = chainDigest, lastChunkHash = lastChunkHash,
            updatedAt = updatedAt, expiresAt = expiresAt
        )
        return 1
    }

    override suspend fun replaceIncomingForRestart(
        transferId: String, token: String, expectedGeneration: Long,
        expectedStorageKind: String, expectedStorageValue: String,
        expectedNextChunkIndex: Int, newGeneration: Long, newStorageKind: String,
        newStorageValue: String, displayName: String, mimeType: String,
        fileSize: Long, chunkSize: Int, chainDigest: ByteArray,
        lastChunkHash: ByteArray, now: Long, expiresAt: Long
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, expectedGeneration, expectedStorageKind, expectedStorageValue, expectedNextChunkIndex)) return 0
        incoming[transferId] = current.copy(
            displayName = displayName, mimeType = mimeType, fileSize = fileSize,
            chunkSize = chunkSize, confirmedBytes = 0, nextChunkIndex = 0,
            chainDigest = chainDigest, lastChunkHash = lastChunkHash,
            retiredStorageKind = current.storageKind, retiredStorageValue = current.storageValue,
            storageKind = newStorageKind, storageValue = newStorageValue,
            generation = newGeneration, sessionClaimedAt = now, updatedAt = now, expiresAt = expiresAt
        )
        return 1
    }

    override suspend fun clearRetiredIncoming(
        transferId: String, token: String, generation: Long, storageKind: String,
        storageValue: String, nextChunkIndex: Int, retiredKind: String, retiredValue: String
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, generation, storageKind, storageValue, nextChunkIndex) ||
            current.retiredStorageKind != retiredKind || current.retiredStorageValue != retiredValue
        ) return 0
        incoming[transferId] = current.copy(retiredStorageKind = null, retiredStorageValue = null)
        return 1
    }

    override suspend fun deleteOwnedIncoming(
        transferId: String, token: String, generation: Long, storageKind: String,
        storageValue: String, nextChunkIndex: Int
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, generation, storageKind, storageValue, nextChunkIndex)) return 0
        incoming.remove(transferId)
        return 1
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
        if (
            current == null ||
            current.cleanupToken != null ||
            current.nextChunkIndex != expectedNextChunkIndex
        ) return 0
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
        val current = incoming[transferId]?.takeIf { it.cleanupToken == null } ?: return 0
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

    override suspend fun insertOutgoing(entity: OutgoingResumeLinkEntity): Long {
        if (outgoing.values.any { it.sourceUri == entity.sourceUri && it.peerDeviceId == entity.peerDeviceId }) return -1
        outgoing[entity.transferId] = entity
        return 1
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

    override suspend fun markExpiredIncomingClaimed(
        now: Long,
        staleClaimBefore: Long,
        token: String
    ): Int {
        val eligible = incoming.values.filter {
            it.expiresAt <= now &&
                it.sessionToken == null &&
                (it.cleanupToken == null || (it.cleanupClaimedAt ?: Long.MIN_VALUE) < staleClaimBefore)
        }
        eligible.forEach { entity ->
            incoming[entity.transferId] = entity.copy(
                cleanupToken = token,
                cleanupClaimedAt = now
            )
        }
        return eligible.size
    }

    override suspend fun findIncomingByCleanupToken(token: String): List<IncomingCheckpointEntity> =
        incoming.values.filter { it.cleanupToken == token }

    override suspend fun claimExpiredIncoming(
        now: Long,
        staleClaimBefore: Long,
        token: String
    ): List<IncomingCheckpointEntity> {
        markExpiredIncomingClaimed(now, staleClaimBefore, token)
        return findIncomingByCleanupToken(token)
    }

    override suspend fun deleteClaimedIncoming(token: String): Int {
        val ids = incoming.values.filter { it.cleanupToken == token }.map { it.transferId }
        ids.forEach(incoming::remove)
        return ids.size
    }

    override suspend fun releaseClaimedIncoming(token: String): Int {
        val claimed = incoming.values.filter { it.cleanupToken == token }
        claimed.forEach { entity ->
            incoming[entity.transferId] = entity.copy(cleanupToken = null, cleanupClaimedAt = null)
        }
        return claimed.size
    }

    override suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long): Int {
        val ids = outgoing.values.filter { it.updatedAt <= updatedAtCutoff }.map { it.transferId }
        ids.forEach(outgoing::remove)
        return ids.size
    }

    private fun IncomingCheckpointEntity.ownedBy(
        token: String, generation: Long, storageKind: String,
        storageValue: String, nextChunkIndex: Int
    ) = sessionToken == token && this.generation == generation &&
        this.storageKind == storageKind && this.storageValue == storageValue &&
        this.nextChunkIndex == nextChunkIndex

}
