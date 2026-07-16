package com.example.transfer.batch

import androidx.room.Embedded
import androidx.room.Entity
import androidx.room.ForeignKey
import androidx.room.Index
import androidx.room.Relation

object OutgoingBatchState {
    const val PENDING = "PENDING"
    const val ACTIVE = "ACTIVE"
    const val INTERRUPTED = "INTERRUPTED"
}

object OutgoingBatchItemState {
    const val PENDING = "PENDING"
    const val ACTIVE = "ACTIVE"
    const val SUCCEEDED = "SUCCEEDED"
}

data class OutgoingBatch(
    val batchId: String,
    val peerDeviceId: String,
    val state: String,
    val createdAt: Long,
    val updatedAt: Long
)

data class OutgoingBatchItem(
    val batchId: String,
    val position: Int,
    val sourceUri: String,
    val displayName: String,
    val mimeType: String,
    val fileSize: Long,
    val lastModified: Long?,
    val transferId: String?,
    val state: String,
    val updatedAt: Long
)

data class OutgoingBatchSnapshot(
    val batch: OutgoingBatch,
    val items: List<OutgoingBatchItem>
) {
    val pendingItems: List<OutgoingBatchItem>
        get() = items.filter { it.state != OutgoingBatchItemState.SUCCEEDED }
            .sortedBy { it.position }
}

@Entity(tableName = "outgoing_batches")
data class OutgoingBatchEntity(
    @androidx.room.PrimaryKey val batchId: String,
    val peerDeviceId: String,
    val state: String,
    val createdAt: Long,
    val updatedAt: Long
)

@Entity(
    tableName = "outgoing_batch_items",
    primaryKeys = ["batchId", "position"],
    foreignKeys = [
        ForeignKey(
            entity = OutgoingBatchEntity::class,
            parentColumns = ["batchId"],
            childColumns = ["batchId"],
            onDelete = ForeignKey.CASCADE
        )
    ],
    indices = [Index("batchId")]
)
data class OutgoingBatchItemEntity(
    val batchId: String,
    val position: Int,
    val sourceUri: String,
    val displayName: String,
    val mimeType: String,
    val fileSize: Long,
    val lastModified: Long?,
    val transferId: String?,
    val state: String,
    val updatedAt: Long
)

data class OutgoingBatchWithItemsEntity(
    @Embedded val batch: OutgoingBatchEntity,
    @Relation(parentColumn = "batchId", entityColumn = "batchId")
    val items: List<OutgoingBatchItemEntity>
)
