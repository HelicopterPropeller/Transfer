package com.example.transfer.resume

interface ResumeStore {
    suspend fun findIncoming(transferId: String): IncomingCheckpoint?
    suspend fun saveIncoming(checkpoint: IncomingCheckpoint)

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

    suspend fun updateIncomingExpiry(transferId: String, updatedAt: Long, expiresAt: Long): Boolean
    suspend fun deleteIncoming(transferId: String)

    suspend fun findOutgoing(sourceUri: String, peerDeviceId: String): OutgoingResumeLink?
    suspend fun saveOutgoing(link: OutgoingResumeLink)
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
        cleanupClaimedAt = null
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
        expiresAt = expiresAt
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
