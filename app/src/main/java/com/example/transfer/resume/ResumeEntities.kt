package com.example.transfer.resume

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

object ResumeStorageKind {
    const val MEDIA_STORE = "MEDIA_STORE"
    const val LEGACY_FILE = "LEGACY_FILE"
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
    val expiresAt: Long
) {
    val location: ResumeStorageLocation
        get() = ResumeStorageLocation(storageKind, storageValue)
}

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
    val expiresAt: Long
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
