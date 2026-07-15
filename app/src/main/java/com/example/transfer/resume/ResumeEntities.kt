package com.example.transfer.resume

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey
import androidx.room.ColumnInfo

object ResumeStorageKind {
    const val MEDIA_STORE = "MEDIA_STORE"
    const val LEGACY_FILE = "LEGACY_FILE"
}

object IncomingOperationState {
    const val IDLE = "IDLE"
    const val ACTIVE = "ACTIVE"
    const val COMPLETING = "COMPLETING"
}

data class ResumeStorageLocation(
    val kind: String,
    val value: String
)

data class IncomingCheckpoint(
    val transferId: String,
    val senderDeviceId: String,
    val fileName: String,
    val displayName: String,
    val mimeType: String,
    val fileSize: Long,
    val chunkSize: Int,
    val confirmedBytes: Long,
    val nextChunkIndex: Int,
    val chainDigest: ByteArray,
    val lastChunkHash: ByteArray,
    val storageKind: String,
    val storageValue: String,
    val createdAt: Long,
    val updatedAt: Long,
    val expiresAt: Long,
    val generation: Long = 0,
    val sessionToken: String? = null,
    val sessionClaimedAt: Long? = null,
    val retiredStorageKind: String? = null,
    val retiredStorageValue: String? = null,
    val operationState: String = IncomingOperationState.IDLE
) {
    val location: ResumeStorageLocation
        get() = ResumeStorageLocation(storageKind, storageValue)

    val retiredLocation: ResumeStorageLocation?
        get() = retiredStorageKind?.let { kind ->
            retiredStorageValue?.let { value -> ResumeStorageLocation(kind, value) }
        }
}

data class IncomingStagingJournal(
    val transferId: String,
    val stagingId: String,
    val createdAt: Long
)

data class OutgoingResumeLink(
    val transferId: String,
    val sourceUri: String,
    val peerDeviceId: String,
    val fileName: String,
    val mimeType: String,
    val fileSize: Long,
    val lastModified: Long?,
    val chunkSize: Int,
    val createdAt: Long,
    val updatedAt: Long = createdAt
)

@Entity(tableName = "incoming_checkpoints")
data class IncomingCheckpointEntity(
    @PrimaryKey val transferId: String,
    val senderDeviceId: String,
    val fileName: String,
    val displayName: String,
    val mimeType: String,
    val fileSize: Long,
    val chunkSize: Int,
    val confirmedBytes: Long,
    val nextChunkIndex: Int,
    val chainDigest: ByteArray,
    val lastChunkHash: ByteArray,
    val storageKind: String,
    val storageValue: String,
    val createdAt: Long,
    val updatedAt: Long,
    val expiresAt: Long,
    val cleanupToken: String? = null,
    val cleanupClaimedAt: Long? = null,
    @ColumnInfo(defaultValue = "0") val generation: Long = 0,
    @ColumnInfo(defaultValue = "NULL") val sessionToken: String? = null,
    @ColumnInfo(defaultValue = "NULL") val sessionClaimedAt: Long? = null,
    @ColumnInfo(defaultValue = "NULL") val retiredStorageKind: String? = null,
    @ColumnInfo(defaultValue = "NULL") val retiredStorageValue: String? = null,
    @ColumnInfo(defaultValue = "'IDLE'") val operationState: String = IncomingOperationState.IDLE
)

@Entity(tableName = "incoming_staging_journal")
data class IncomingStagingJournalEntity(
    @PrimaryKey val transferId: String,
    val stagingId: String,
    val createdAt: Long
)

@Entity(
    tableName = "outgoing_resume_links",
    indices = [Index(value = ["sourceUri", "peerDeviceId"], unique = true)]
)
data class OutgoingResumeLinkEntity(
    @PrimaryKey val transferId: String,
    val sourceUri: String,
    val peerDeviceId: String,
    val fileName: String,
    val mimeType: String,
    val fileSize: Long,
    val lastModified: Long?,
    val chunkSize: Int,
    val createdAt: Long,
    val updatedAt: Long
)
