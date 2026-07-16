package com.example.transfer.batch

import com.example.transfer.ui.SelectedFile
import java.util.UUID

interface OutgoingBatchStore {
    suspend fun create(
        peerId: String,
        files: List<SelectedFile>,
        now: Long
    ): OutgoingBatchSnapshot

    suspend fun find(batchId: String): OutgoingBatchSnapshot?
    suspend fun latestRecoverable(): OutgoingBatchSnapshot?
    suspend fun findMatching(peerId: String, files: List<SelectedFile>): OutgoingBatchSnapshot?
    suspend fun saveTransferId(
        batchId: String,
        position: Int,
        transferId: String,
        now: Long
    ): Boolean
    suspend fun markBatch(batchId: String, state: String, now: Long): Boolean
    suspend fun markItemActive(batchId: String, position: Int, now: Long): Boolean
    suspend fun markItemPending(batchId: String, position: Int, now: Long): Boolean
    suspend fun markItemSucceeded(batchId: String, position: Int, now: Long): Boolean
    suspend fun recoverInterrupted(now: Long)
    suspend fun delete(batchId: String)
}

class RoomOutgoingBatchStore(
    private val dao: OutgoingBatchDao,
    private val idFactory: () -> String = { UUID.randomUUID().toString() }
) : OutgoingBatchStore {
    override suspend fun create(
        peerId: String,
        files: List<SelectedFile>,
        now: Long
    ): OutgoingBatchSnapshot {
        require(peerId.isNotBlank())
        require(files.isNotEmpty())
        val batchId = idFactory()
        val batch = OutgoingBatchEntity(
            batchId = batchId,
            peerDeviceId = peerId,
            state = OutgoingBatchState.PENDING,
            createdAt = now,
            updatedAt = now
        )
        val items = files.mapIndexed { position, file ->
            OutgoingBatchItemEntity(
                batchId = batchId,
                position = position,
                sourceUri = file.uri,
                displayName = file.displayName,
                mimeType = file.mimeType,
                fileSize = file.size,
                lastModified = file.lastModified,
                transferId = null,
                state = OutgoingBatchItemState.PENDING,
                updatedAt = now
            )
        }
        dao.createBatch(batch, items)
        return OutgoingBatchWithItemsEntity(batch, items).toDomain()
    }

    override suspend fun find(batchId: String): OutgoingBatchSnapshot? =
        dao.findBatch(batchId)?.toDomain()

    override suspend fun latestRecoverable(): OutgoingBatchSnapshot? =
        dao.findRecoverableBatches().asSequence()
            .map { it.toDomain() }
            .firstOrNull { it.pendingItems.isNotEmpty() }

    override suspend fun findMatching(
        peerId: String,
        files: List<SelectedFile>
    ): OutgoingBatchSnapshot? = dao.findRecoverableBatches(peerId)
        .asSequence()
        .map { it.toDomain() }
        .firstOrNull { snapshot ->
            val pending = snapshot.pendingItems
            val original = snapshot.items.sortedBy { it.position }
            pending.matches(files) || original.matches(files)
        }

    override suspend fun saveTransferId(
        batchId: String,
        position: Int,
        transferId: String,
        now: Long
    ): Boolean = dao.updateTransferId(batchId, position, transferId, now) > 0

    override suspend fun markBatch(batchId: String, state: String, now: Long): Boolean =
        dao.updateBatchState(batchId, state, now) > 0

    override suspend fun markItemActive(batchId: String, position: Int, now: Long): Boolean =
        dao.updateItemAndTouch(batchId, position, OutgoingBatchItemState.ACTIVE, now) > 0

    override suspend fun markItemPending(batchId: String, position: Int, now: Long): Boolean =
        dao.updateItemAndTouch(batchId, position, OutgoingBatchItemState.PENDING, now) > 0

    override suspend fun markItemSucceeded(batchId: String, position: Int, now: Long): Boolean =
        dao.updateItemAndTouch(batchId, position, OutgoingBatchItemState.SUCCEEDED, now) > 0

    override suspend fun recoverInterrupted(now: Long) {
        dao.recoverInterrupted(now)
    }

    override suspend fun delete(batchId: String) {
        dao.deleteBatch(batchId)
    }

    private fun OutgoingBatchItem.matches(file: SelectedFile): Boolean =
        sourceUri == file.uri && displayName == file.displayName &&
            mimeType == file.mimeType && fileSize == file.size &&
            lastModified == file.lastModified

    private fun List<OutgoingBatchItem>.matches(files: List<SelectedFile>): Boolean =
        size == files.size && zip(files).all { (item, file) -> item.matches(file) }

    private fun OutgoingBatchWithItemsEntity.toDomain() = OutgoingBatchSnapshot(
        batch = OutgoingBatch(
            batch.batchId,
            batch.peerDeviceId,
            batch.state,
            batch.createdAt,
            batch.updatedAt
        ),
        items = items.sortedBy { it.position }.map { item ->
            OutgoingBatchItem(
                item.batchId,
                item.position,
                item.sourceUri,
                item.displayName,
                item.mimeType,
                item.fileSize,
                item.lastModified,
                item.transferId,
                item.state,
                item.updatedAt
            )
        }
    )
}
