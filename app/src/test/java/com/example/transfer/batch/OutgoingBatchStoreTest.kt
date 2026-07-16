package com.example.transfer.batch

import com.example.transfer.ui.SelectedFile
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class OutgoingBatchStoreTest {
    @Test
    fun `create stores every selected item in order`() = runBlocking {
        val dao = FakeOutgoingBatchDao()
        val store = RoomOutgoingBatchStore(dao, idFactory = { "batch-1" })

        val batch = store.create("peer-1", files("a", "b", "c"), now = 10L)

        assertEquals("batch-1", batch.batch.batchId)
        assertEquals(listOf("a", "b", "c"), batch.items.map { it.displayName })
        assertEquals(listOf(0, 1, 2), batch.items.map { it.position })
        assertEquals(1, dao.batches.size)
        assertEquals(3, dao.items.size)
    }

    @Test
    fun `matching compares only unfinished files in original order`() = runBlocking {
        val dao = FakeOutgoingBatchDao()
        val store = RoomOutgoingBatchStore(dao, idFactory = { "batch-1" })
        val originalFiles = files("a", "b", "c")
        val created = store.create("peer-1", originalFiles, now = 10L)
        store.markItemSucceeded(created.batch.batchId, position = 0, now = 20L)
        val pendingFiles = created.items.drop(1).map { item ->
            SelectedFile(
                item.sourceUri,
                item.displayName,
                item.mimeType,
                item.fileSize,
                item.lastModified
            )
        }

        val matching = store.findMatching("peer-1", pendingFiles)
        val matchingFromOriginalSelection = store.findMatching("peer-1", originalFiles)

        assertEquals(created.batch.batchId, matching?.batch?.batchId)
        assertEquals(created.batch.batchId, matchingFromOriginalSelection?.batch?.batchId)
        assertNull(store.findMatching("peer-1", pendingFiles.reversed()))
    }

    @Test
    fun `recovery excludes succeeded files and resets active files`() = runBlocking {
        val dao = FakeOutgoingBatchDao()
        val store = RoomOutgoingBatchStore(dao, idFactory = { "batch-1" })
        val created = store.create("peer-1", files("a", "b", "c"), now = 10L)
        store.markItemSucceeded(created.batch.batchId, position = 0, now = 20L)
        store.markItemActive(created.batch.batchId, position = 1, now = 20L)
        store.markBatch(created.batch.batchId, OutgoingBatchState.ACTIVE, now = 20L)

        store.recoverInterrupted(now = 30L)
        val recovered = store.latestRecoverable()

        assertNotNull(recovered)
        assertEquals(OutgoingBatchState.INTERRUPTED, recovered?.batch?.state)
        assertEquals(listOf("b", "c"), recovered?.pendingItems?.map { it.displayName })
        assertTrue(recovered?.pendingItems?.all { it.state == OutgoingBatchItemState.PENDING } == true)
    }

    private fun files(vararg names: String) = names.mapIndexed { index, name ->
        SelectedFile(
            uri = "content://source/$name",
            displayName = name,
            mimeType = "application/octet-stream",
            size = index + 1L,
            lastModified = 100L + index
        )
    }
}

private class FakeOutgoingBatchDao : OutgoingBatchDao {
    val batches = linkedMapOf<String, OutgoingBatchEntity>()
    val items = linkedMapOf<Pair<String, Int>, OutgoingBatchItemEntity>()

    override suspend fun insertBatch(entity: OutgoingBatchEntity) {
        check(batches.putIfAbsent(entity.batchId, entity) == null)
    }

    override suspend fun insertItems(entities: List<OutgoingBatchItemEntity>) {
        entities.forEach { entity ->
            val key = entity.batchId to entity.position
            check(items.putIfAbsent(key, entity) == null)
        }
    }

    override suspend fun findBatch(batchId: String): OutgoingBatchWithItemsEntity? =
        batches[batchId]?.let { batch -> aggregate(batch) }

    override suspend fun findRecoverableBatches(): List<OutgoingBatchWithItemsEntity> =
        batches.values.sortedByDescending { it.updatedAt }.map(::aggregate)

    override suspend fun findRecoverableBatches(peerDeviceId: String): List<OutgoingBatchWithItemsEntity> =
        batches.values.filter { it.peerDeviceId == peerDeviceId }
            .sortedByDescending { it.updatedAt }.map(::aggregate)

    override suspend fun updateTransferId(
        batchId: String,
        position: Int,
        transferId: String,
        updatedAt: Long
    ): Int = updateItem(batchId, position) { it.copy(transferId = transferId, updatedAt = updatedAt) }

    override suspend fun updateBatchState(batchId: String, state: String, updatedAt: Long): Int {
        val current = batches[batchId] ?: return 0
        batches[batchId] = current.copy(state = state, updatedAt = updatedAt)
        return 1
    }

    override suspend fun updateItemState(
        batchId: String,
        position: Int,
        state: String,
        updatedAt: Long
    ): Int = updateItem(batchId, position) { it.copy(state = state, updatedAt = updatedAt) }

    override suspend fun touchBatch(batchId: String, updatedAt: Long): Int {
        val current = batches[batchId] ?: return 0
        batches[batchId] = current.copy(updatedAt = updatedAt)
        return 1
    }

    override suspend fun resetActiveItems(updatedAt: Long): Int {
        val active = items.filterValues { it.state == OutgoingBatchItemState.ACTIVE }.keys
        active.forEach { key ->
            items[key] = requireNotNull(items[key]).copy(
                state = OutgoingBatchItemState.PENDING,
                updatedAt = updatedAt
            )
        }
        return active.size
    }

    override suspend fun resetActiveBatches(updatedAt: Long): Int {
        val active = batches.filterValues { it.state == OutgoingBatchState.ACTIVE }.keys
        active.forEach { key ->
            batches[key] = requireNotNull(batches[key]).copy(
                state = OutgoingBatchState.INTERRUPTED,
                updatedAt = updatedAt
            )
        }
        return active.size
    }

    override suspend fun deleteCompletedBatches(): Int {
        val completed = batches.keys.filter { batchId ->
            items.values.filter { it.batchId == batchId }
                .all { it.state == OutgoingBatchItemState.SUCCEEDED }
        }
        completed.forEach { batchId -> deleteBatch(batchId) }
        return completed.size
    }

    override suspend fun deleteBatch(batchId: String): Int {
        if (batches.remove(batchId) == null) return 0
        items.entries.removeAll { it.key.first == batchId }
        return 1
    }

    private fun aggregate(batch: OutgoingBatchEntity) = OutgoingBatchWithItemsEntity(
        batch = batch,
        items = items.values.filter { it.batchId == batch.batchId }.sortedBy { it.position }
    )

    private fun updateItem(
        batchId: String,
        position: Int,
        transform: (OutgoingBatchItemEntity) -> OutgoingBatchItemEntity
    ): Int {
        val key = batchId to position
        val current = items[key] ?: return 0
        items[key] = transform(current)
        return 1
    }
}
