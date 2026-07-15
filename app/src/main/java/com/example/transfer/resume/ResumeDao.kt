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

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertOutgoing(entity: OutgoingResumeLinkEntity)

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
