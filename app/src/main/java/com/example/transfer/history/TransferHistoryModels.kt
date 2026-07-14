package com.example.transfer.history

import kotlinx.coroutines.flow.Flow

enum class TransferDirection { SEND, RECEIVE }

enum class TransferHistoryStatus { IN_PROGRESS, SUCCESS, FAILED, CANCELLED, INTERRUPTED }

data class TransferHistoryDraft(
    val direction: TransferDirection,
    val fileName: String,
    val fileSize: Long,
    val mimeType: String,
    val peerId: String? = null,
    val peerName: String? = null,
    val peerAddress: String? = null,
    val sourceUri: String? = null
)

data class TransferHistoryEntry(
    val id: Long,
    val direction: TransferDirection,
    val fileName: String,
    val fileSize: Long,
    val mimeType: String,
    val peerId: String?,
    val peerName: String?,
    val peerAddress: String?,
    val status: TransferHistoryStatus,
    val startedAt: Long,
    val finishedAt: Long?,
    val errorMessage: String?,
    val sourceUri: String?,
    val receivedUri: String?
)

interface TransferHistoryStore {
    fun observeAll(): Flow<List<TransferHistoryEntry>>

    suspend fun start(draft: TransferHistoryDraft): Long?

    suspend fun finish(
        id: Long,
        status: TransferHistoryStatus,
        errorMessage: String? = null,
        receivedUri: String? = null
    ): Boolean

    suspend fun interruptActive(): Int

    suspend fun delete(id: Long): Boolean

    suspend fun clear(): Boolean
}
