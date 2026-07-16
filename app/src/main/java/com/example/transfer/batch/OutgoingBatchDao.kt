package com.example.transfer.batch

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.Query
import androidx.room.Transaction

@Dao
interface OutgoingBatchDao {
    @Insert
    suspend fun insertBatch(entity: OutgoingBatchEntity)

    @Insert
    suspend fun insertItems(entities: List<OutgoingBatchItemEntity>)

    @Transaction
    suspend fun createBatch(
        batch: OutgoingBatchEntity,
        items: List<OutgoingBatchItemEntity>
    ) {
        insertBatch(batch)
        insertItems(items)
    }

    @Transaction
    @Query("SELECT * FROM outgoing_batches WHERE batchId = :batchId LIMIT 1")
    suspend fun findBatch(batchId: String): OutgoingBatchWithItemsEntity?

    @Transaction
    @Query("SELECT * FROM outgoing_batches ORDER BY updatedAt DESC, batchId DESC")
    suspend fun findRecoverableBatches(): List<OutgoingBatchWithItemsEntity>

    @Transaction
    @Query(
        "SELECT * FROM outgoing_batches WHERE peerDeviceId = :peerDeviceId " +
            "ORDER BY updatedAt DESC, batchId DESC"
    )
    suspend fun findRecoverableBatches(peerDeviceId: String): List<OutgoingBatchWithItemsEntity>

    @Query(
        "UPDATE outgoing_batch_items SET transferId = :transferId, updatedAt = :updatedAt " +
            "WHERE batchId = :batchId AND position = :position AND state != 'SUCCEEDED'"
    )
    suspend fun updateTransferId(
        batchId: String,
        position: Int,
        transferId: String,
        updatedAt: Long
    ): Int

    @Query(
        "UPDATE outgoing_batches SET state = :state, updatedAt = :updatedAt " +
            "WHERE batchId = :batchId"
    )
    suspend fun updateBatchState(batchId: String, state: String, updatedAt: Long): Int

    @Query(
        "UPDATE outgoing_batch_items SET state = :state, updatedAt = :updatedAt " +
            "WHERE batchId = :batchId AND position = :position"
    )
    suspend fun updateItemState(
        batchId: String,
        position: Int,
        state: String,
        updatedAt: Long
    ): Int

    @Query("UPDATE outgoing_batches SET updatedAt = :updatedAt WHERE batchId = :batchId")
    suspend fun touchBatch(batchId: String, updatedAt: Long): Int

    @Transaction
    suspend fun updateItemAndTouch(
        batchId: String,
        position: Int,
        state: String,
        updatedAt: Long
    ): Int {
        val updated = updateItemState(batchId, position, state, updatedAt)
        if (updated > 0) touchBatch(batchId, updatedAt)
        return updated
    }

    @Query(
        "UPDATE outgoing_batch_items SET state = 'PENDING', updatedAt = :updatedAt " +
            "WHERE state = 'ACTIVE'"
    )
    suspend fun resetActiveItems(updatedAt: Long): Int

    @Query(
        "UPDATE outgoing_batches SET state = 'INTERRUPTED', updatedAt = :updatedAt " +
            "WHERE state = 'ACTIVE'"
    )
    suspend fun resetActiveBatches(updatedAt: Long): Int

    @Query(
        "DELETE FROM outgoing_batches WHERE NOT EXISTS (" +
            "SELECT 1 FROM outgoing_batch_items " +
            "WHERE outgoing_batch_items.batchId = outgoing_batches.batchId " +
            "AND outgoing_batch_items.state != 'SUCCEEDED')"
    )
    suspend fun deleteCompletedBatches(): Int

    @Transaction
    suspend fun recoverInterrupted(updatedAt: Long) {
        resetActiveItems(updatedAt)
        resetActiveBatches(updatedAt)
        deleteCompletedBatches()
    }

    @Query("DELETE FROM outgoing_batches WHERE batchId = :batchId")
    suspend fun deleteBatch(batchId: String): Int
}
