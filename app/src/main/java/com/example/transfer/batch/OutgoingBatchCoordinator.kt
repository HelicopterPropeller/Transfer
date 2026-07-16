package com.example.transfer.batch

import com.example.transfer.ui.SelectedFile

class OutgoingBatchCoordinator(
    private val store: OutgoingBatchStore,
    private val clock: () -> Long = System::currentTimeMillis
) {
    suspend fun acquire(
        peerId: String,
        files: List<SelectedFile>
    ): OutgoingBatchSnapshot = store.findMatching(peerId, files)
        ?: store.create(peerId, files, clock())

    suspend fun latestRecoverable(): OutgoingBatchSnapshot? = store.latestRecoverable()

    suspend fun find(batchId: String): OutgoingBatchSnapshot? = store.find(batchId)

    suspend fun recoverInterrupted() {
        store.recoverInterrupted(clock())
    }

    suspend fun saveTransferId(item: OutgoingBatchItem, transferId: String): Boolean =
        store.saveTransferId(item.batchId, item.position, transferId, clock())

    suspend fun markBatch(batchId: String, state: String): Boolean =
        store.markBatch(batchId, state, clock())

    suspend fun markItemActive(item: OutgoingBatchItem): Boolean =
        store.markItemActive(item.batchId, item.position, clock())

    suspend fun markItemPending(item: OutgoingBatchItem): Boolean =
        store.markItemPending(item.batchId, item.position, clock())

    suspend fun markItemSucceeded(item: OutgoingBatchItem): Boolean =
        store.markItemSucceeded(item.batchId, item.position, clock())

    suspend fun delete(batchId: String) {
        store.delete(batchId)
    }
}
