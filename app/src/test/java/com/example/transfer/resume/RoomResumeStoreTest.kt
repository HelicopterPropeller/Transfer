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
    fun `restart replacement persists every offered identity field`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        val original = incoming("t1").copy(
            senderDeviceId = "old-sender",
            fileName = "old.bin",
            sessionToken = "owner",
            sessionClaimedAt = 20L,
            operationState = IncomingOperationState.ACTIVE
        )
        store.saveIncoming(original)
        val replacement = original.copy(
            senderDeviceId = "new-sender",
            fileName = "new.bin",
            displayName = "new (1).bin",
            mimeType = "application/new",
            fileSize = 64L,
            chunkSize = 16,
            confirmedBytes = 0L,
            nextChunkIndex = 0,
            storageValue = "content://pending/replacement",
            generation = original.generation + 1,
            retiredStorageKind = original.storageKind,
            retiredStorageValue = original.storageValue
        )

        assertTrue(store.replaceIncomingForRestart(original, replacement, "owner"))

        val persisted = requireNotNull(store.findIncoming("t1"))
        assertEquals(replacement.senderDeviceId, persisted.senderDeviceId)
        assertEquals(replacement.fileName, persisted.fileName)
        assertEquals(replacement.displayName, persisted.displayName)
        assertEquals(replacement.mimeType, persisted.mimeType)
        assertEquals(replacement.fileSize, persisted.fileSize)
        assertEquals(replacement.chunkSize, persisted.chunkSize)
    }

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

    @Test
    fun `heartbeat advances active lease and completing is never stale-cleared`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        val original = incoming("t1")
        store.saveIncoming(original)
        val active = requireNotNull(store.acquireIncomingSession(original, "session", 20L, 200L))

        assertTrue(store.heartbeatIncomingSession(active, "session", 30L, 300L))
        val refreshed = requireNotNull(store.findIncoming("t1"))
        assertEquals(30L, refreshed.sessionClaimedAt)
        assertTrue(store.beginIncomingCompletion(refreshed, "session", ByteArray(32), 40L, 400L))

        assertEquals(0, store.clearStaleIncomingSessions(Long.MAX_VALUE))
        assertFalse(store.updateIncomingExpiry("t1", 50L, 500L))
        assertFalse(
            store.commitIncomingChunk(
                "t1", refreshed.nextChunkIndex, 16L, 2,
                byteArrayOf(3), byteArrayOf(4), 50L, 500L
            )
        )
        assertEquals(IncomingOperationState.COMPLETING, store.findIncoming("t1")?.operationState)
    }

    @Test
    fun `completion transaction maps durable receipt and removes completing checkpoint`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        val digest = ByteArray(32) { 6 }
        val completing = incoming("t1").copy(
            operationState = IncomingOperationState.COMPLETING,
            completingFinalDigest = digest
        )
        store.saveIncoming(completing)
        val receipt = CompletedReceipt(
            transferId = completing.transferId,
            senderDeviceId = completing.senderDeviceId,
            fileName = completing.fileName,
            mimeType = completing.mimeType,
            fileSize = completing.fileSize,
            chunkSize = completing.chunkSize,
            finalDigest = digest,
            publishedUri = "content://received/a.bin",
            publishedName = completing.displayName,
            completedAt = 100L,
            expiresAt = 200L
        )

        assertTrue(store.finishIncomingCompletion(completing, receipt))

        assertNull(store.findIncoming("t1"))
        val stored = requireNotNull(store.findCompletedReceipt("t1", now = 150L))
        assertArrayEquals(digest, stored.finalDigest)
        assertEquals(receipt.publishedUri, stored.publishedUri)
        assertNull(store.findCompletedReceipt("t1", now = 200L))
    }

    @Test
    fun `legacy completing digest is persisted only for exact full checkpoint`() = runBlocking {
        val store = RoomResumeStore(FakeResumeDao())
        val completing = incoming("t1").copy(
            confirmedBytes = 32L,
            nextChunkIndex = 4,
            operationState = IncomingOperationState.COMPLETING,
            completingFinalDigest = null
        )
        store.saveIncoming(completing)
        val digest = ByteArray(32) { 3 }

        val recovered = store.persistRecoveredCompletingDigest(
            completing, digest, now = 50L, expiresAt = 500L
        )

        assertArrayEquals(digest, recovered?.completingFinalDigest)
        assertEquals(50L, recovered?.updatedAt)
        assertNull(
            store.persistRecoveredCompletingDigest(
                completing, ByteArray(32) { 9 }, now = 60L, expiresAt = 600L
            )
        )
    }

    @Test
    fun `staging journal survives store reopen until exact record is removed`() = runBlocking {
        val dao = FakeResumeDao()
        val firstStore = RoomResumeStore(dao)
        val journal = IncomingStagingJournal("t1", "t1-1", 10L)

        assertTrue(firstStore.insertStagingJournal(journal))
        assertFalse(firstStore.insertStagingJournal(journal.copy(stagingId = "other")))

        val reopenedStore = RoomResumeStore(dao)
        assertEquals(listOf(journal), reopenedStore.findStagingJournals())
        assertFalse(reopenedStore.deleteStagingJournal(journal.copy(stagingId = "other")))
        assertTrue(reopenedStore.deleteStagingJournal(journal))
        assertTrue(reopenedStore.findStagingJournals().isEmpty())
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
    private val staging = linkedMapOf<String, IncomingStagingJournalEntity>()

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
            sessionToken = token, sessionClaimedAt = now,
            operationState = IncomingOperationState.ACTIVE,
            updatedAt = now, expiresAt = expiresAt
        )
        return 1
    }

    override suspend fun releaseIncomingSession(
        transferId: String, token: String, generation: Long, storageKind: String,
        storageValue: String, nextChunkIndex: Int
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, generation, storageKind, storageValue, nextChunkIndex)) return 0
        incoming[transferId] = current.copy(
            sessionToken = null, sessionClaimedAt = null,
            operationState = IncomingOperationState.IDLE
        )
        return 1
    }

    override suspend fun clearStaleIncomingSessions(staleClaimBefore: Long): Int {
        val rows = incoming.values.filter {
            it.operationState == IncomingOperationState.ACTIVE && it.sessionToken != null &&
                (it.sessionClaimedAt ?: Long.MAX_VALUE) < staleClaimBefore
        }
        rows.forEach {
            incoming[it.transferId] = it.copy(
                sessionToken = null, sessionClaimedAt = null,
                operationState = IncomingOperationState.IDLE
            )
        }
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

    override suspend fun heartbeatIncomingSession(
        transferId: String, token: String, generation: Long, storageKind: String,
        storageValue: String, nextChunkIndex: Int, now: Long, expiresAt: Long
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, generation, storageKind, storageValue, nextChunkIndex) ||
            current.operationState != IncomingOperationState.ACTIVE
        ) return 0
        incoming[transferId] = current.copy(
            sessionClaimedAt = now, updatedAt = now, expiresAt = expiresAt
        )
        return 1
    }

    private val receipts = linkedMapOf<String, CompletedReceiptEntity>()

    override suspend fun beginIncomingCompletion(
        transferId: String, token: String, generation: Long, storageKind: String,
        storageValue: String, nextChunkIndex: Int, finalDigest: ByteArray,
        now: Long, expiresAt: Long
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, generation, storageKind, storageValue, nextChunkIndex) ||
            current.operationState != IncomingOperationState.ACTIVE
        ) return 0
        incoming[transferId] = current.copy(
            operationState = IncomingOperationState.COMPLETING,
            sessionClaimedAt = now, updatedAt = now, expiresAt = expiresAt,
            completingFinalDigest = finalDigest
        )
        return 1
    }

    override suspend fun findCompletingIncoming(): List<IncomingCheckpointEntity> =
        incoming.values.filter { it.operationState == IncomingOperationState.COMPLETING }

    override suspend fun persistRecoveredCompletingDigest(
        transferId: String,
        generation: Long,
        storageKind: String,
        storageValue: String,
        finalDigest: ByteArray,
        now: Long,
        expiresAt: Long
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (current.operationState != IncomingOperationState.COMPLETING ||
            current.completingFinalDigest != null || current.confirmedBytes != current.fileSize ||
            current.generation != generation || current.storageKind != storageKind ||
            current.storageValue != storageValue
        ) return 0
        incoming[transferId] = current.copy(
            completingFinalDigest = finalDigest, updatedAt = now, expiresAt = expiresAt
        )
        return 1
    }

    override suspend fun deleteCompletingIncoming(
        transferId: String, generation: Long, storageKind: String, storageValue: String,
        finalDigest: ByteArray
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (current.operationState != IncomingOperationState.COMPLETING ||
            current.generation != generation || current.storageKind != storageKind ||
            current.storageValue != storageValue ||
            current.completingFinalDigest?.contentEquals(finalDigest) != true
        ) return 0
        incoming.remove(transferId)
        return 1
    }

    override suspend fun findCompletedReceipt(transferId: String): CompletedReceiptEntity? =
        receipts[transferId]

    override suspend fun deleteExpiredCompletedReceipt(transferId: String, now: Long): Int {
        val current = receipts[transferId] ?: return 0
        if (current.expiresAt > now) return 0
        receipts.remove(transferId)
        return 1
    }

    override suspend fun insertCompletedReceipt(receipt: CompletedReceiptEntity): Long {
        if (receipts.containsKey(receipt.transferId)) return -1
        receipts[receipt.transferId] = receipt
        return 1
    }

    override suspend fun deleteExpiredCompletedReceipts(now: Long): Int {
        val expired = receipts.values.filter { it.expiresAt <= now }.map { it.transferId }
        expired.forEach(receipts::remove)
        return expired.size
    }

    override suspend fun clearRetiredIncomingForRecovery(
        transferId: String, generation: Long, storageKind: String, storageValue: String,
        retiredKind: String, retiredValue: String
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (current.generation != generation || current.storageKind != storageKind ||
            current.storageValue != storageValue || current.retiredStorageKind != retiredKind ||
            current.retiredStorageValue != retiredValue
        ) return 0
        incoming[transferId] = current.copy(
            retiredStorageKind = null, retiredStorageValue = null
        )
        return 1
    }

    override suspend fun insertStagingJournal(entity: IncomingStagingJournalEntity): Long {
        if (staging.containsKey(entity.transferId)) return -1
        staging[entity.transferId] = entity
        return 1
    }

    override suspend fun findStagingJournals(): List<IncomingStagingJournalEntity> =
        staging.values.toList()

    override suspend fun deleteStagingJournal(transferId: String, stagingId: String): Int {
        val current = staging[transferId] ?: return 0
        if (current.stagingId != stagingId) return 0
        staging.remove(transferId)
        return 1
    }

    override suspend fun replaceIncomingForRestart(
        transferId: String, token: String, expectedGeneration: Long,
        expectedStorageKind: String, expectedStorageValue: String,
        expectedNextChunkIndex: Int, newGeneration: Long, newStorageKind: String,
        newStorageValue: String, displayName: String, senderDeviceId: String,
        fileName: String, mimeType: String,
        fileSize: Long, chunkSize: Int, chainDigest: ByteArray,
        lastChunkHash: ByteArray, now: Long, expiresAt: Long
    ): Int {
        val current = incoming[transferId] ?: return 0
        if (!current.ownedBy(token, expectedGeneration, expectedStorageKind, expectedStorageValue, expectedNextChunkIndex)) return 0
        incoming[transferId] = current.copy(
            senderDeviceId = senderDeviceId, fileName = fileName,
            displayName = displayName, mimeType = mimeType, fileSize = fileSize,
            chunkSize = chunkSize, confirmedBytes = 0, nextChunkIndex = 0,
            chainDigest = chainDigest, lastChunkHash = lastChunkHash,
            retiredStorageKind = current.storageKind, retiredStorageValue = current.storageValue,
            storageKind = newStorageKind, storageValue = newStorageValue,
            generation = newGeneration, sessionClaimedAt = now, updatedAt = now, expiresAt = expiresAt
        )
        return 1
    }

    override suspend fun findIncomingWithRetiredStorage(): List<IncomingCheckpointEntity> =
        incoming.values.filter {
            it.retiredStorageKind != null && it.retiredStorageValue != null
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
            current.sessionToken != null ||
            current.operationState != IncomingOperationState.IDLE ||
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
        val current = incoming[transferId]?.takeIf {
            it.cleanupToken == null && it.sessionToken == null &&
                it.operationState == IncomingOperationState.IDLE
        } ?: return 0
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
