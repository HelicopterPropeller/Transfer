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
        WHERE transferId = :transferId
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
        SELECT * FROM incoming_checkpoints
        WHERE expiresAt <= :now
        ORDER BY expiresAt ASC, transferId ASC
        """
    )
    suspend fun findExpiredIncoming(now: Long): List<IncomingCheckpointEntity>

    @Query("DELETE FROM incoming_checkpoints WHERE expiresAt <= :now")
    suspend fun deleteExpiredIncoming(now: Long): Int

    @Query("DELETE FROM outgoing_resume_links WHERE updatedAt <= :updatedAtCutoff")
    suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long): Int

    @Transaction
    suspend fun deleteExpired(now: Long, outgoingUpdatedAtCutoff: Long): Int =
        deleteExpiredIncoming(now) + deleteExpiredOutgoing(outgoingUpdatedAtCutoff)
}
