package com.example.transfer.history

import androidx.room.Entity
import androidx.room.PrimaryKey

@Entity(tableName = "transfer_history")
data class TransferHistoryEntity(
    @PrimaryKey(autoGenerate = true)
    val id: Long = 0,
    val direction: String,
    val fileName: String,
    val fileSize: Long,
    val mimeType: String,
    val peerId: String?,
    val peerName: String?,
    val peerAddress: String?,
    val status: String,
    val startedAt: Long,
    val finishedAt: Long?,
    val errorMessage: String?,
    val sourceUri: String?,
    val receivedUri: String?
)
