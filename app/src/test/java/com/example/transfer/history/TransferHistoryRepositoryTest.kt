package com.example.transfer.history

import java.io.IOException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class TransferHistoryRepositoryTest {
    @Test
    fun `start finish delete and interrupt preserve terminal semantics`() = runBlocking {
        val dao = FakeTransferHistoryDao()
        val repository = TransferHistoryRepository(dao, now = { 20L })
        val first = repository.start(draft("a.bin"))!!
        val second = repository.start(draft("b.bin"))!!

        repository.finish(first, TransferHistoryStatus.SUCCESS)
        repository.interruptActive()

        val rows = repository.observeAll().first()
        assertEquals(TransferHistoryStatus.INTERRUPTED, rows[0].status)
        assertEquals(TransferHistoryStatus.SUCCESS, rows[1].status)
        repository.delete(first)
        assertEquals(listOf(second), repository.observeAll().first().map { it.id })
        repository.clear()
        assertTrue(repository.observeAll().first().isEmpty())
    }

    @Test
    fun `dao failures are best effort`() = runBlocking {
        val repository = TransferHistoryRepository(ThrowingTransferHistoryDao(), now = { 20L })
        assertNull(repository.start(draft("a.bin")))
        repository.finish(1, TransferHistoryStatus.FAILED, "broken")
        repository.delete(1)
        repository.clear()
        Unit
    }

    private fun draft(fileName: String) = TransferHistoryDraft(
        direction = TransferDirection.SEND,
        fileName = fileName,
        fileSize = 10L,
        mimeType = "application/octet-stream"
    )
}

private class FakeTransferHistoryDao : TransferHistoryDao {
    private val rows = MutableStateFlow<List<TransferHistoryEntity>>(emptyList())
    private var nextId = 1L

    override fun observeAll(): Flow<List<TransferHistoryEntity>> = rows

    override suspend fun insert(entity: TransferHistoryEntity): Long {
        val id = nextId++
        rows.update { current -> sorted(current + entity.copy(id = id)) }
        return id
    }

    override suspend fun finish(
        id: Long,
        status: String,
        finishedAt: Long,
        errorMessage: String?,
        receivedUri: String?
    ): Int {
        var updated = 0
        rows.update { current ->
            sorted(current.map { entity ->
                if (entity.id == id && entity.status == TransferHistoryStatus.IN_PROGRESS.name) {
                    updated = 1
                    entity.copy(
                        status = status,
                        finishedAt = finishedAt,
                        errorMessage = errorMessage,
                        receivedUri = receivedUri
                    )
                } else {
                    entity
                }
            })
        }
        return updated
    }

    override suspend fun interruptActive(finishedAt: Long, message: String): Int {
        var updated = 0
        rows.update { current ->
            sorted(current.map { entity ->
                if (entity.status == TransferHistoryStatus.IN_PROGRESS.name) {
                    updated += 1
                    entity.copy(
                        status = TransferHistoryStatus.INTERRUPTED.name,
                        finishedAt = finishedAt,
                        errorMessage = message
                    )
                } else {
                    entity
                }
            })
        }
        return updated
    }

    override suspend fun delete(id: Long): Int {
        var deleted = 0
        rows.update { current ->
            current.filterNot { entity ->
                (entity.id == id).also { matches -> if (matches) deleted += 1 }
            }
        }
        return deleted
    }

    override suspend fun clear(): Int {
        val deleted = rows.value.size
        rows.value = emptyList()
        return deleted
    }

    private fun sorted(entities: List<TransferHistoryEntity>) =
        entities.sortedWith(compareByDescending<TransferHistoryEntity> { it.startedAt }.thenByDescending { it.id })
}

private class ThrowingTransferHistoryDao : TransferHistoryDao {
    override fun observeAll(): Flow<List<TransferHistoryEntity>> = flow {
        throw unavailable()
    }

    override suspend fun insert(entity: TransferHistoryEntity): Long = throw unavailable()

    override suspend fun finish(
        id: Long,
        status: String,
        finishedAt: Long,
        errorMessage: String?,
        receivedUri: String?
    ): Int = throw unavailable()

    override suspend fun interruptActive(finishedAt: Long, message: String): Int = throw unavailable()

    override suspend fun delete(id: Long): Int = throw unavailable()

    override suspend fun clear(): Int = throw unavailable()

    private fun unavailable() = IOException("database unavailable")
}
