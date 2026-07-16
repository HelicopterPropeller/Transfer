package com.example.transfer.batch

import com.example.transfer.ui.SelectedFile
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test

class OutgoingBatchCoordinatorTest {
    @Test
    fun `acquire reuses an exactly matching unfinished batch`() = runBlocking {
        val store = RecordingBatchStore()
        val coordinator = OutgoingBatchCoordinator(store, clock = { 10L })
        val files = files(size = 4L)

        val first = coordinator.acquire("peer-1", files)
        val second = coordinator.acquire("peer-1", files)

        assertEquals(first.batch.batchId, second.batch.batchId)
        assertEquals(1, store.createdCount)
    }

    @Test
    fun `acquire creates a new batch when file identity changes`() = runBlocking {
        val store = RecordingBatchStore()
        val coordinator = OutgoingBatchCoordinator(store, clock = { 10L })

        val first = coordinator.acquire("peer-1", files(size = 4L))
        val second = coordinator.acquire("peer-1", files(size = 5L))

        assertNotEquals(first.batch.batchId, second.batch.batchId)
        assertEquals(2, store.createdCount)
    }

    private fun files(size: Long) = listOf(
        SelectedFile("content://a", "a.bin", "application/octet-stream", size, 20L)
    )
}

private class RecordingBatchStore : OutgoingBatchStore {
    private val batches = mutableListOf<OutgoingBatchSnapshot>()
    var createdCount = 0
        private set

    override suspend fun create(
        peerId: String,
        files: List<SelectedFile>,
        now: Long
    ): OutgoingBatchSnapshot {
        createdCount++
        val id = "batch-$createdCount"
        return OutgoingBatchSnapshot(
            OutgoingBatch(id, peerId, OutgoingBatchState.PENDING, now, now),
            files.mapIndexed { position, file ->
                OutgoingBatchItem(
                    id, position, file.uri, file.displayName, file.mimeType,
                    file.size, file.lastModified, null, OutgoingBatchItemState.PENDING, now
                )
            }
        ).also(batches::add)
    }

    override suspend fun find(batchId: String) = batches.singleOrNull { it.batch.batchId == batchId }
    override suspend fun latestRecoverable() = batches.lastOrNull()
    override suspend fun findMatching(peerId: String, files: List<SelectedFile>) =
        batches.lastOrNull { snapshot ->
            snapshot.batch.peerDeviceId == peerId &&
                snapshot.pendingItems.map { it.toSelectedFile() } == files
        }

    override suspend fun saveTransferId(
        batchId: String,
        position: Int,
        transferId: String,
        now: Long
    ) = error("unused")

    override suspend fun markBatch(batchId: String, state: String, now: Long) = error("unused")
    override suspend fun markItemActive(batchId: String, position: Int, now: Long) = error("unused")
    override suspend fun markItemPending(batchId: String, position: Int, now: Long) = error("unused")
    override suspend fun markItemSucceeded(batchId: String, position: Int, now: Long) = error("unused")
    override suspend fun recoverInterrupted(now: Long) = Unit
    override suspend fun delete(batchId: String) = Unit

    private fun OutgoingBatchItem.toSelectedFile() = SelectedFile(
        sourceUri, displayName, mimeType, fileSize, lastModified
    )
}
