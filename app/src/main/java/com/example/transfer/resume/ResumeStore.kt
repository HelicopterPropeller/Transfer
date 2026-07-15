package com.example.transfer.resume

interface ResumeStore {
    suspend fun findIncoming(transferId: String): IncomingCheckpoint?
    suspend fun saveIncoming(checkpoint: IncomingCheckpoint)
    suspend fun insertIncoming(checkpoint: IncomingCheckpoint): Boolean =
        error("insertIncoming is not implemented")
    suspend fun acquireIncomingSession(
        expected: IncomingCheckpoint,
        token: String,
        now: Long,
        expiresAt: Long
    ): IncomingCheckpoint? = error("acquireIncomingSession is not implemented")
    suspend fun releaseIncomingSession(expected: IncomingCheckpoint, token: String): Boolean =
        error("releaseIncomingSession is not implemented")
    suspend fun clearStaleIncomingSessions(staleClaimBefore: Long): Int =
        error("clearStaleIncomingSessions is not implemented")
    suspend fun replaceIncomingForRestart(
        expected: IncomingCheckpoint,
        replacement: IncomingCheckpoint,
        token: String
    ): Boolean = error("replaceIncomingForRestart is not implemented")
    suspend fun clearRetiredIncoming(expected: IncomingCheckpoint, token: String): Boolean =
        error("clearRetiredIncoming is not implemented")
    suspend fun deleteOwnedIncoming(expected: IncomingCheckpoint, token: String): Boolean =
        error("deleteOwnedIncoming is not implemented")
    suspend fun heartbeatIncomingSession(
        expected: IncomingCheckpoint,
        token: String,
        now: Long,
        expiresAt: Long
    ): Boolean = error("heartbeatIncomingSession is not implemented")
    suspend fun beginIncomingCompletion(
        expected: IncomingCheckpoint,
        token: String,
        now: Long,
        expiresAt: Long
    ): Boolean = error("beginIncomingCompletion is not implemented")
    suspend fun findCompletingIncoming(): List<IncomingCheckpoint> = emptyList()
    suspend fun deleteCompletingIncoming(expected: IncomingCheckpoint): Boolean =
        error("deleteCompletingIncoming is not implemented")
    suspend fun clearRetiredIncomingForRecovery(expected: IncomingCheckpoint): Boolean =
        error("clearRetiredIncomingForRecovery is not implemented")
    suspend fun insertStagingJournal(journal: IncomingStagingJournal): Boolean =
        error("insertStagingJournal is not implemented")
    suspend fun findStagingJournals(): List<IncomingStagingJournal> = emptyList()
    suspend fun deleteStagingJournal(journal: IncomingStagingJournal): Boolean =
        error("deleteStagingJournal is not implemented")

    suspend fun commitIncomingChunk(
        transferId: String,
        expectedNextChunkIndex: Int,
        confirmedBytes: Long,
        nextChunkIndex: Int,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray,
        updatedAt: Long,
        expiresAt: Long
    ): Boolean
    suspend fun commitOwnedIncomingChunk(
        expected: IncomingCheckpoint,
        token: String,
        confirmedBytes: Long,
        nextChunkIndex: Int,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray,
        updatedAt: Long,
        expiresAt: Long
    ): Boolean = error("commitOwnedIncomingChunk is not implemented")

    suspend fun updateIncomingExpiry(transferId: String, updatedAt: Long, expiresAt: Long): Boolean
    suspend fun deleteIncoming(transferId: String)

    suspend fun findOutgoing(sourceUri: String, peerDeviceId: String): OutgoingResumeLink?
    suspend fun saveOutgoing(link: OutgoingResumeLink)
    suspend fun resolveOutgoing(candidate: OutgoingResumeLink): OutgoingResumeLink =
        error("resolveOutgoing is not implemented")
    suspend fun updateOutgoingTimestamp(transferId: String, updatedAt: Long): Boolean
    suspend fun deleteOutgoing(transferId: String)

    suspend fun deleteCompleted(transferId: String)
    suspend fun claimExpiredIncoming(
        now: Long,
        staleClaimBefore: Long,
        token: String
    ): List<IncomingCheckpoint>
    suspend fun deleteClaimedIncoming(token: String): Int
    suspend fun releaseClaimedIncoming(token: String): Int
    suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long): Int
}

class RoomResumeStore(
    private val dao: ResumeDao
) : ResumeStore {
    override suspend fun findIncoming(transferId: String): IncomingCheckpoint? =
        dao.findIncoming(transferId)?.toDomain()

    override suspend fun saveIncoming(checkpoint: IncomingCheckpoint) {
        dao.upsertIncoming(checkpoint.toEntity())
    }

    override suspend fun insertIncoming(checkpoint: IncomingCheckpoint): Boolean =
        dao.insertIncoming(checkpoint.toEntity()) != -1L

    override suspend fun acquireIncomingSession(
        expected: IncomingCheckpoint,
        token: String,
        now: Long,
        expiresAt: Long
    ): IncomingCheckpoint? {
        val acquired = dao.acquireIncomingSession(
            transferId = expected.transferId,
            storageKind = expected.storageKind,
            storageValue = expected.storageValue,
            generation = expected.generation,
            nextChunkIndex = expected.nextChunkIndex,
            confirmedBytes = expected.confirmedBytes,
            chainDigest = expected.chainDigest,
            lastChunkHash = expected.lastChunkHash,
            token = token,
            now = now,
            expiresAt = expiresAt
        ) > 0
        return if (acquired) dao.findIncoming(expected.transferId)?.toDomain() else null
    }

    override suspend fun releaseIncomingSession(expected: IncomingCheckpoint, token: String): Boolean =
        dao.releaseIncomingSession(
            expected.transferId, token, expected.generation, expected.storageKind,
            expected.storageValue, expected.nextChunkIndex
        ) > 0

    override suspend fun clearStaleIncomingSessions(staleClaimBefore: Long): Int =
        dao.clearStaleIncomingSessions(staleClaimBefore)

    override suspend fun replaceIncomingForRestart(
        expected: IncomingCheckpoint,
        replacement: IncomingCheckpoint,
        token: String
    ): Boolean = dao.replaceIncomingForRestart(
        transferId = expected.transferId,
        token = token,
        expectedGeneration = expected.generation,
        expectedStorageKind = expected.storageKind,
        expectedStorageValue = expected.storageValue,
        expectedNextChunkIndex = expected.nextChunkIndex,
        newGeneration = replacement.generation,
        newStorageKind = replacement.storageKind,
        newStorageValue = replacement.storageValue,
        displayName = replacement.displayName,
        mimeType = replacement.mimeType,
        fileSize = replacement.fileSize,
        chunkSize = replacement.chunkSize,
        chainDigest = replacement.chainDigest,
        lastChunkHash = replacement.lastChunkHash,
        now = replacement.updatedAt,
        expiresAt = replacement.expiresAt
    ) > 0

    override suspend fun clearRetiredIncoming(expected: IncomingCheckpoint, token: String): Boolean {
        val retired = expected.retiredLocation ?: return true
        return dao.clearRetiredIncoming(
            expected.transferId, token, expected.generation, expected.storageKind,
            expected.storageValue, expected.nextChunkIndex, retired.kind, retired.value
        ) > 0
    }

    override suspend fun deleteOwnedIncoming(expected: IncomingCheckpoint, token: String): Boolean =
        dao.deleteOwnedIncoming(
            expected.transferId, token, expected.generation, expected.storageKind,
            expected.storageValue, expected.nextChunkIndex
        ) > 0

    override suspend fun heartbeatIncomingSession(
        expected: IncomingCheckpoint,
        token: String,
        now: Long,
        expiresAt: Long
    ): Boolean = dao.heartbeatIncomingSession(
        expected.transferId, token, expected.generation, expected.storageKind,
        expected.storageValue, expected.nextChunkIndex, now, expiresAt
    ) > 0

    override suspend fun beginIncomingCompletion(
        expected: IncomingCheckpoint,
        token: String,
        now: Long,
        expiresAt: Long
    ): Boolean = dao.beginIncomingCompletion(
        expected.transferId, token, expected.generation, expected.storageKind,
        expected.storageValue, expected.nextChunkIndex, now, expiresAt
    ) > 0

    override suspend fun findCompletingIncoming(): List<IncomingCheckpoint> =
        dao.findCompletingIncoming().map { it.toDomain() }

    override suspend fun deleteCompletingIncoming(expected: IncomingCheckpoint): Boolean =
        dao.deleteCompletingIncoming(
            expected.transferId, expected.generation, expected.storageKind, expected.storageValue
        ) > 0

    override suspend fun clearRetiredIncomingForRecovery(expected: IncomingCheckpoint): Boolean {
        val retired = expected.retiredLocation ?: return true
        return dao.clearRetiredIncomingForRecovery(
            expected.transferId, expected.generation, expected.storageKind, expected.storageValue,
            retired.kind, retired.value
        ) > 0
    }

    override suspend fun insertStagingJournal(journal: IncomingStagingJournal): Boolean =
        dao.insertStagingJournal(journal.toEntity()) != -1L

    override suspend fun findStagingJournals(): List<IncomingStagingJournal> =
        dao.findStagingJournals().map { it.toDomain() }

    override suspend fun deleteStagingJournal(journal: IncomingStagingJournal): Boolean =
        dao.deleteStagingJournal(journal.transferId, journal.stagingId) > 0

    override suspend fun commitIncomingChunk(
        transferId: String,
        expectedNextChunkIndex: Int,
        confirmedBytes: Long,
        nextChunkIndex: Int,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray,
        updatedAt: Long,
        expiresAt: Long
    ): Boolean = dao.commitIncomingChunk(
        transferId = transferId,
        expectedNextChunkIndex = expectedNextChunkIndex,
        confirmedBytes = confirmedBytes,
        nextChunkIndex = nextChunkIndex,
        chainDigest = chainDigest,
        lastChunkHash = lastChunkHash,
        updatedAt = updatedAt,
        expiresAt = expiresAt
    ) > 0

    override suspend fun commitOwnedIncomingChunk(
        expected: IncomingCheckpoint,
        token: String,
        confirmedBytes: Long,
        nextChunkIndex: Int,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray,
        updatedAt: Long,
        expiresAt: Long
    ): Boolean = dao.commitOwnedIncomingChunk(
        expected.transferId, token, expected.generation, expected.storageKind,
        expected.storageValue, expected.nextChunkIndex, confirmedBytes,
        nextChunkIndex, chainDigest, lastChunkHash, updatedAt, expiresAt
    ) > 0

    override suspend fun updateIncomingExpiry(
        transferId: String,
        updatedAt: Long,
        expiresAt: Long
    ): Boolean = dao.updateIncomingExpiry(transferId, updatedAt, expiresAt) > 0

    override suspend fun deleteIncoming(transferId: String) {
        dao.deleteIncoming(transferId)
    }

    override suspend fun findOutgoing(
        sourceUri: String,
        peerDeviceId: String
    ): OutgoingResumeLink? = dao.findOutgoing(sourceUri, peerDeviceId)?.toDomain()

    override suspend fun saveOutgoing(link: OutgoingResumeLink) {
        dao.upsertOutgoing(link.toEntity())
    }

    override suspend fun resolveOutgoing(candidate: OutgoingResumeLink): OutgoingResumeLink =
        dao.resolveOutgoing(candidate.toEntity()).toDomain()

    override suspend fun updateOutgoingTimestamp(transferId: String, updatedAt: Long): Boolean =
        dao.updateOutgoingTimestamp(transferId, updatedAt) > 0

    override suspend fun deleteOutgoing(transferId: String) {
        dao.deleteOutgoing(transferId)
    }

    override suspend fun deleteCompleted(transferId: String) {
        dao.deleteCompleted(transferId)
    }

    override suspend fun claimExpiredIncoming(
        now: Long,
        staleClaimBefore: Long,
        token: String
    ): List<IncomingCheckpoint> =
        dao.claimExpiredIncoming(now, staleClaimBefore, token).map { it.toDomain() }

    override suspend fun deleteClaimedIncoming(token: String): Int =
        dao.deleteClaimedIncoming(token)

    override suspend fun releaseClaimedIncoming(token: String): Int =
        dao.releaseClaimedIncoming(token)

    override suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long): Int =
        dao.deleteExpiredOutgoing(updatedAtCutoff)

    private fun IncomingCheckpoint.toEntity() = IncomingCheckpointEntity(
        transferId = transferId,
        senderDeviceId = senderDeviceId,
        fileName = fileName,
        displayName = displayName,
        mimeType = mimeType,
        fileSize = fileSize,
        chunkSize = chunkSize,
        confirmedBytes = confirmedBytes,
        nextChunkIndex = nextChunkIndex,
        chainDigest = chainDigest,
        lastChunkHash = lastChunkHash,
        storageKind = storageKind,
        storageValue = storageValue,
        createdAt = createdAt,
        updatedAt = updatedAt,
        expiresAt = expiresAt,
        cleanupToken = null,
        cleanupClaimedAt = null,
        generation = generation,
        sessionToken = sessionToken,
        sessionClaimedAt = sessionClaimedAt,
        retiredStorageKind = retiredStorageKind,
        retiredStorageValue = retiredStorageValue,
        operationState = operationState
    )

    private fun IncomingCheckpointEntity.toDomain() = IncomingCheckpoint(
        transferId = transferId,
        senderDeviceId = senderDeviceId,
        fileName = fileName,
        displayName = displayName,
        mimeType = mimeType,
        fileSize = fileSize,
        chunkSize = chunkSize,
        confirmedBytes = confirmedBytes,
        nextChunkIndex = nextChunkIndex,
        chainDigest = chainDigest,
        lastChunkHash = lastChunkHash,
        storageKind = storageKind,
        storageValue = storageValue,
        createdAt = createdAt,
        updatedAt = updatedAt,
        expiresAt = expiresAt,
        generation = generation,
        sessionToken = sessionToken,
        sessionClaimedAt = sessionClaimedAt,
        retiredStorageKind = retiredStorageKind,
        retiredStorageValue = retiredStorageValue,
        operationState = operationState
    )

    private fun IncomingStagingJournal.toEntity() = IncomingStagingJournalEntity(
        transferId = transferId,
        stagingId = stagingId,
        createdAt = createdAt
    )

    private fun IncomingStagingJournalEntity.toDomain() = IncomingStagingJournal(
        transferId = transferId,
        stagingId = stagingId,
        createdAt = createdAt
    )

    private fun OutgoingResumeLink.toEntity() = OutgoingResumeLinkEntity(
        transferId = transferId,
        sourceUri = sourceUri,
        peerDeviceId = peerDeviceId,
        fileName = fileName,
        mimeType = mimeType,
        fileSize = fileSize,
        lastModified = lastModified,
        chunkSize = chunkSize,
        createdAt = createdAt,
        updatedAt = updatedAt
    )

    private fun OutgoingResumeLinkEntity.toDomain() = OutgoingResumeLink(
        transferId = transferId,
        sourceUri = sourceUri,
        peerDeviceId = peerDeviceId,
        fileName = fileName,
        mimeType = mimeType,
        fileSize = fileSize,
        lastModified = lastModified,
        chunkSize = chunkSize,
        createdAt = createdAt,
        updatedAt = updatedAt
    )
}
