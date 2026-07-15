package com.example.transfer.resume

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.Transaction

@Dao
interface ResumeDao {
    @Query("SELECT * FROM incoming_checkpoints WHERE transferId = :transferId LIMIT 1")
    suspend fun findIncoming(transferId: String): IncomingCheckpointEntity?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertIncoming(entity: IncomingCheckpointEntity)

    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insertIncoming(entity: IncomingCheckpointEntity): Long

    @Query(
        """
        UPDATE incoming_checkpoints
        SET sessionToken = :token, sessionClaimedAt = :now,
            updatedAt = :now, expiresAt = :expiresAt
        WHERE transferId = :transferId
          AND storageKind = :storageKind AND storageValue = :storageValue
          AND generation = :generation AND nextChunkIndex = :nextChunkIndex
          AND confirmedBytes = :confirmedBytes
          AND chainDigest = :chainDigest AND lastChunkHash = :lastChunkHash
          AND sessionToken IS NULL AND cleanupToken IS NULL
        """
    )
    suspend fun acquireIncomingSession(
        transferId: String,
        storageKind: String,
        storageValue: String,
        generation: Long,
        nextChunkIndex: Int,
        confirmedBytes: Long,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray,
        token: String,
        now: Long,
        expiresAt: Long
    ): Int

    @Query(
        """
        UPDATE incoming_checkpoints
        SET sessionToken = NULL, sessionClaimedAt = NULL
        WHERE transferId = :transferId AND sessionToken = :token
          AND generation = :generation AND storageKind = :storageKind
          AND storageValue = :storageValue AND nextChunkIndex = :nextChunkIndex
        """
    )
    suspend fun releaseIncomingSession(
        transferId: String, token: String, generation: Long,
        storageKind: String, storageValue: String, nextChunkIndex: Int
    ): Int

    @Query(
        """
        UPDATE incoming_checkpoints SET sessionToken = NULL, sessionClaimedAt = NULL
        WHERE sessionToken IS NOT NULL AND sessionClaimedAt < :staleClaimBefore
        """
    )
    suspend fun clearStaleIncomingSessions(staleClaimBefore: Long): Int

    @Query(
        """
        UPDATE incoming_checkpoints
        SET confirmedBytes = :confirmedBytes, nextChunkIndex = :nextChunkIndex,
            chainDigest = :chainDigest, lastChunkHash = :lastChunkHash,
            updatedAt = :updatedAt, expiresAt = :expiresAt
        WHERE transferId = :transferId AND sessionToken = :token
          AND generation = :generation AND storageKind = :storageKind
          AND storageValue = :storageValue AND nextChunkIndex = :expectedNextChunkIndex
          AND cleanupToken IS NULL
        """
    )
    suspend fun commitOwnedIncomingChunk(
        transferId: String, token: String, generation: Long,
        storageKind: String, storageValue: String, expectedNextChunkIndex: Int,
        confirmedBytes: Long, nextChunkIndex: Int, chainDigest: ByteArray,
        lastChunkHash: ByteArray, updatedAt: Long, expiresAt: Long
    ): Int

    @Query(
        """
        UPDATE incoming_checkpoints
        SET displayName = :displayName, mimeType = :mimeType, fileSize = :fileSize,
            chunkSize = :chunkSize, confirmedBytes = 0, nextChunkIndex = 0,
            chainDigest = :chainDigest, lastChunkHash = :lastChunkHash,
            storageKind = :newStorageKind, storageValue = :newStorageValue,
            retiredStorageKind = storageKind, retiredStorageValue = storageValue,
            generation = :newGeneration, sessionClaimedAt = :now,
            updatedAt = :now, expiresAt = :expiresAt
        WHERE transferId = :transferId AND sessionToken = :token
          AND generation = :expectedGeneration AND storageKind = :expectedStorageKind
          AND storageValue = :expectedStorageValue AND nextChunkIndex = :expectedNextChunkIndex
          AND cleanupToken IS NULL
        """
    )
    suspend fun replaceIncomingForRestart(
        transferId: String, token: String, expectedGeneration: Long,
        expectedStorageKind: String, expectedStorageValue: String,
        expectedNextChunkIndex: Int, newGeneration: Long,
        newStorageKind: String, newStorageValue: String, displayName: String,
        mimeType: String, fileSize: Long, chunkSize: Int,
        chainDigest: ByteArray, lastChunkHash: ByteArray,
        now: Long, expiresAt: Long
    ): Int

    @Query(
        """
        UPDATE incoming_checkpoints
        SET retiredStorageKind = NULL, retiredStorageValue = NULL
        WHERE transferId = :transferId AND sessionToken = :token
          AND generation = :generation AND storageKind = :storageKind
          AND storageValue = :storageValue AND nextChunkIndex = :nextChunkIndex
          AND retiredStorageKind = :retiredKind AND retiredStorageValue = :retiredValue
        """
    )
    suspend fun clearRetiredIncoming(
        transferId: String, token: String, generation: Long,
        storageKind: String, storageValue: String, nextChunkIndex: Int,
        retiredKind: String, retiredValue: String
    ): Int

    @Query(
        """
        DELETE FROM incoming_checkpoints
        WHERE transferId = :transferId AND sessionToken = :token
          AND generation = :generation AND storageKind = :storageKind
          AND storageValue = :storageValue AND nextChunkIndex = :nextChunkIndex
        """
    )
    suspend fun deleteOwnedIncoming(
        transferId: String, token: String, generation: Long,
        storageKind: String, storageValue: String, nextChunkIndex: Int
    ): Int

    @Transaction
    @Query(
        """
        UPDATE incoming_checkpoints
        SET confirmedBytes = :confirmedBytes,
            nextChunkIndex = :nextChunkIndex,
            chainDigest = :chainDigest,
            lastChunkHash = :lastChunkHash,
            updatedAt = :updatedAt,
            expiresAt = :expiresAt
        WHERE transferId = :transferId
          AND nextChunkIndex = :expectedNextChunkIndex
          AND cleanupToken IS NULL
        """
    )
    suspend fun commitIncomingChunk(
        transferId: String,
        expectedNextChunkIndex: Int,
        confirmedBytes: Long,
        nextChunkIndex: Int,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray,
        updatedAt: Long,
        expiresAt: Long
    ): Int

    @Query(
        """
        UPDATE incoming_checkpoints
        SET updatedAt = :updatedAt, expiresAt = :expiresAt
        WHERE transferId = :transferId AND cleanupToken IS NULL
        """
    )
    suspend fun updateIncomingExpiry(transferId: String, updatedAt: Long, expiresAt: Long): Int

    @Query("DELETE FROM incoming_checkpoints WHERE transferId = :transferId")
    suspend fun deleteIncoming(transferId: String): Int

    @Query(
        """
        SELECT * FROM outgoing_resume_links
        WHERE sourceUri = :sourceUri AND peerDeviceId = :peerDeviceId
        LIMIT 1
        """
    )
    suspend fun findOutgoing(sourceUri: String, peerDeviceId: String): OutgoingResumeLinkEntity?

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun upsertOutgoing(entity: OutgoingResumeLinkEntity)

    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insertOutgoing(entity: OutgoingResumeLinkEntity): Long

    @Transaction
    suspend fun resolveOutgoing(candidate: OutgoingResumeLinkEntity): OutgoingResumeLinkEntity {
        val existing = findOutgoing(candidate.sourceUri, candidate.peerDeviceId)
        if (existing != null && existing.sameIdentity(candidate)) {
            updateOutgoingTimestamp(existing.transferId, candidate.updatedAt)
            return findOutgoing(candidate.sourceUri, candidate.peerDeviceId) ?: existing
        }
        if (existing != null) deleteOutgoing(existing.transferId)
        insertOutgoing(candidate)
        return checkNotNull(findOutgoing(candidate.sourceUri, candidate.peerDeviceId))
    }

    private fun OutgoingResumeLinkEntity.sameIdentity(other: OutgoingResumeLinkEntity): Boolean =
        fileName == other.fileName && mimeType == other.mimeType &&
            fileSize == other.fileSize && lastModified == other.lastModified &&
            chunkSize == other.chunkSize

    @Query(
        """
        UPDATE outgoing_resume_links
        SET updatedAt = :updatedAt
        WHERE transferId = :transferId
        """
    )
    suspend fun updateOutgoingTimestamp(transferId: String, updatedAt: Long): Int

    @Query("DELETE FROM outgoing_resume_links WHERE transferId = :transferId")
    suspend fun deleteOutgoing(transferId: String): Int

    @Transaction
    suspend fun deleteCompleted(transferId: String) {
        deleteIncoming(transferId)
        deleteOutgoing(transferId)
    }

    @Query(
        """
        UPDATE incoming_checkpoints
        SET cleanupToken = :token, cleanupClaimedAt = :now
        WHERE expiresAt <= :now
          AND sessionToken IS NULL
          AND (cleanupToken IS NULL OR cleanupClaimedAt < :staleClaimBefore)
        """
    )
    suspend fun markExpiredIncomingClaimed(now: Long, staleClaimBefore: Long, token: String): Int

    @Query(
        """
        SELECT * FROM incoming_checkpoints
        WHERE cleanupToken = :token
        ORDER BY expiresAt ASC, transferId ASC
        """
    )
    suspend fun findIncomingByCleanupToken(token: String): List<IncomingCheckpointEntity>

    @Transaction
    suspend fun claimExpiredIncoming(
        now: Long,
        staleClaimBefore: Long,
        token: String
    ): List<IncomingCheckpointEntity> {
        markExpiredIncomingClaimed(now, staleClaimBefore, token)
        return findIncomingByCleanupToken(token)
    }

    @Query("DELETE FROM incoming_checkpoints WHERE cleanupToken = :token")
    suspend fun deleteClaimedIncoming(token: String): Int

    @Query(
        """
        UPDATE incoming_checkpoints
        SET cleanupToken = NULL, cleanupClaimedAt = NULL
        WHERE cleanupToken = :token
        """
    )
    suspend fun releaseClaimedIncoming(token: String): Int

    @Query("DELETE FROM outgoing_resume_links WHERE updatedAt <= :updatedAtCutoff")
    suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long): Int
}
