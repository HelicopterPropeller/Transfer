package com.example.transfer.history

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.Query
import kotlinx.coroutines.flow.Flow

@Dao
interface TransferHistoryDao {
    @Query("SELECT * FROM transfer_history ORDER BY startedAt DESC, id DESC")
    fun observeAll(): Flow<List<TransferHistoryEntity>>

    @Insert
    suspend fun insert(entity: TransferHistoryEntity): Long

    @Query(
        """
        UPDATE transfer_history
        SET status = :status, finishedAt = :finishedAt,
            errorMessage = :errorMessage, receivedUri = :receivedUri
        WHERE id = :id AND status = 'IN_PROGRESS'
        """
    )
    suspend fun finish(
        id: Long,
        status: String,
        finishedAt: Long,
        errorMessage: String?,
        receivedUri: String?
    ): Int

    @Query(
        """
        UPDATE transfer_history
        SET status = 'INTERRUPTED', finishedAt = :finishedAt,
            errorMessage = :message
        WHERE status = 'IN_PROGRESS'
        """
    )
    suspend fun interruptActive(finishedAt: Long, message: String): Int

    @Query("DELETE FROM transfer_history WHERE id = :id")
    suspend fun delete(id: Long): Int

    @Query("DELETE FROM transfer_history")
    suspend fun clear(): Int
}
