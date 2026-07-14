package com.example.transfer.history

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

class TransferHistoryRepository(
    private val dao: TransferHistoryDao,
    private val now: () -> Long = System::currentTimeMillis
) : TransferHistoryStore {
    override fun observeAll(): Flow<List<TransferHistoryEntry>> =
        dao.observeAll().map { rows -> rows.map { entity -> entity.toDomain() } }

    override suspend fun start(draft: TransferHistoryDraft): Long? {
        if (draft.fileName.isBlank() || draft.fileSize < 0) return null

        return try {
            dao.insert(
                TransferHistoryEntity(
                    direction = draft.direction.name,
                    fileName = draft.fileName,
                    fileSize = draft.fileSize,
                    mimeType = draft.mimeType,
                    peerId = draft.peerId,
                    peerName = draft.peerName,
                    peerAddress = draft.peerAddress,
                    status = TransferHistoryStatus.IN_PROGRESS.name,
                    startedAt = now(),
                    finishedAt = null,
                    errorMessage = null,
                    sourceUri = draft.sourceUri,
                    receivedUri = null
                )
            )
        } catch (error: CancellationException) {
            throw error
        } catch (_: Exception) {
            null
        }
    }

    override suspend fun finish(
        id: Long,
        status: TransferHistoryStatus,
        errorMessage: String?,
        receivedUri: String?
    ): Boolean {
        if (status == TransferHistoryStatus.IN_PROGRESS) return false

        return try {
            dao.finish(
                id = id,
                status = status.name,
                finishedAt = now(),
                errorMessage = errorMessage?.take(MAX_ERROR_MESSAGE_LENGTH),
                receivedUri = receivedUri
            ) > 0
        } catch (error: CancellationException) {
            throw error
        } catch (_: Exception) {
            false
        }
    }

    override suspend fun interruptActive(): Int = try {
        dao.interruptActive(now(), INTERRUPTED_MESSAGE)
    } catch (error: CancellationException) {
        throw error
    } catch (_: Exception) {
        0
    }

    override suspend fun delete(id: Long): Boolean = try {
        dao.delete(id) > 0
    } catch (error: CancellationException) {
        throw error
    } catch (_: Exception) {
        false
    }

    override suspend fun clear(): Boolean = try {
        dao.clear()
        true
    } catch (error: CancellationException) {
        throw error
    } catch (_: Exception) {
        false
    }

    private fun TransferHistoryEntity.toDomain() = TransferHistoryEntry(
        id = id,
        direction = TransferDirection.valueOf(direction),
        fileName = fileName,
        fileSize = fileSize,
        mimeType = mimeType,
        peerId = peerId,
        peerName = peerName,
        peerAddress = peerAddress,
        status = TransferHistoryStatus.valueOf(status),
        startedAt = startedAt,
        finishedAt = finishedAt,
        errorMessage = errorMessage,
        sourceUri = sourceUri,
        receivedUri = receivedUri
    )

    private companion object {
        const val MAX_ERROR_MESSAGE_LENGTH = 500
        const val INTERRUPTED_MESSAGE = "Transfer interrupted"
    }
}
