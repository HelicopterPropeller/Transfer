package com.example.transfer.resume

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.PrefixDigest
import com.example.transfer.protocol.PrefixDigestScanner
import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferOffer
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.storage.ResumableFileHandle
import com.example.transfer.storage.ResumableIncomingFileStore
import com.example.transfer.storage.StoredFileLocation
import com.example.transfer.transfer.SendFileSource
import java.io.IOException
import java.util.UUID

data class PreparedTransfer(
    val offer: TransferOffer,
    val source: SendFileSource,
    val resumeStatus: ResumeStatus
)

data class ResumeCandidate(
    val prepared: PreparedTransfer,
    val status: ResumeStatus
)

data class IncomingResumeSession(
    val handle: ResumableFileHandle,
    val checkpoint: IncomingCheckpoint,
    val prefix: PrefixDigest
)

class ResumeValidationException(message: String, cause: Throwable? = null) : IOException(message, cause)

class CheckpointConflictException(message: String) : IOException(message)

class ResumeCoordinator(
    private val store: ResumeStore,
    private val files: ResumableIncomingFileStore,
    private val clock: () -> Long = System::currentTimeMillis,
    private val newTransferId: () -> String = { UUID.randomUUID().toString() }
) {
    suspend fun prepareOutgoing(
        source: SendFileSource,
        peerDeviceId: String,
        senderDeviceId: String
    ): PreparedTransfer {
        val sourceUri = requireNotNull(source.sourceUri) { "A stable source URI is required for resume" }
        val now = clock()
        val existing = store.findOutgoing(sourceUri, peerDeviceId)
        val saved = if (existing != null && existing.matches(source)) {
            existing.copy(updatedAt = now).also { store.saveOutgoing(it) }
        } else {
            if (existing != null) store.deleteOutgoing(existing.transferId)
            OutgoingResumeLink(
                transferId = newTransferId(),
                sourceUri = sourceUri,
                peerDeviceId = peerDeviceId,
                fileName = source.displayName,
                mimeType = source.mimeType,
                fileSize = source.length,
                lastModified = source.lastModified,
                chunkSize = TransferProtocol.CHUNK_SIZE,
                createdAt = now,
                updatedAt = now
            ).also { store.saveOutgoing(it) }
        }
        return PreparedTransfer(
            offer = TransferOffer(
                transferId = saved.transferId,
                senderDeviceId = senderDeviceId,
                fileName = saved.fileName,
                mimeType = saved.mimeType,
                fileSize = saved.fileSize,
                chunkSize = saved.chunkSize
            ),
            source = source,
            resumeStatus = ResumeStatus(ResumeState.NONE)
        )
    }

    suspend fun queryIncoming(offer: TransferOffer): ResumeStatus {
        val checkpoint = store.findIncoming(offer.transferId) ?: return ResumeStatus(ResumeState.NONE)
        if (!checkpoint.isResumableFor(offer)) return ResumeStatus(ResumeState.INVALID)
        return checkpoint.toStatus(ResumeState.AVAILABLE)
    }

    fun validatePrefix(status: ResumeStatus, scanned: PrefixDigest): Boolean =
        status.state == ResumeState.AVAILABLE &&
            status.confirmedBytes == scanned.scannedBytes &&
            status.nextChunkIndex == scanned.nextChunkIndex &&
            status.chainDigest.contentEquals(scanned.chainDigest) &&
            status.lastChunkHash.contentEquals(scanned.lastChunkHash)

    suspend fun openIncoming(
        offer: TransferOffer,
        mode: TransferStartMode,
        expectedStatus: ResumeStatus
    ): IncomingResumeSession = when (mode) {
        TransferStartMode.NEW -> openNew(offer, expectedStatus)
        TransferStartMode.RESTART -> openRestart(offer, expectedStatus)
        TransferStartMode.RESUME -> openResume(offer, expectedStatus)
    }

    suspend fun commitChunk(
        session: IncomingResumeSession,
        confirmedBytes: Long,
        nextChunkIndex: Int,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray
    ): IncomingResumeSession {
        session.handle.force()
        val now = clock()
        val expiresAt = now + RETENTION_MILLIS
        val committed = store.commitIncomingChunk(
            transferId = session.checkpoint.transferId,
            expectedNextChunkIndex = session.checkpoint.nextChunkIndex,
            confirmedBytes = confirmedBytes,
            nextChunkIndex = nextChunkIndex,
            chainDigest = chainDigest,
            lastChunkHash = lastChunkHash,
            updatedAt = now,
            expiresAt = expiresAt
        )
        if (!committed) throw CheckpointConflictException("Incoming checkpoint changed during commit")

        return session.copy(
            checkpoint = session.checkpoint.copy(
                confirmedBytes = confirmedBytes,
                nextChunkIndex = nextChunkIndex,
                chainDigest = chainDigest.copyOf(),
                lastChunkHash = lastChunkHash.copyOf(),
                updatedAt = now,
                expiresAt = expiresAt
            ),
            prefix = PrefixDigest(
                scannedBytes = confirmedBytes,
                nextChunkIndex = nextChunkIndex,
                chainDigest = chainDigest.copyOf(),
                lastChunkHash = lastChunkHash.copyOf(),
                wholeDigest = session.prefix.wholeDigest
            )
        )
    }

    suspend fun completeIncoming(session: IncomingResumeSession): String? {
        val published = files.publish(session.handle)
        store.deleteIncoming(session.checkpoint.transferId)
        return published
    }

    suspend fun completeOutgoing(transferId: String) {
        store.deleteOutgoing(transferId)
    }

    private suspend fun openNew(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): IncomingResumeSession {
        if (expectedStatus.state != ResumeState.NONE || store.findIncoming(offer.transferId) != null) {
            throw CheckpointConflictException("Incoming checkpoint already exists")
        }
        val handle = files.create(offer.transferId, offer.fileName, offer.mimeType)
        val prefix = emptyPrefix()
        val checkpoint = newCheckpoint(offer, handle, prefix)
        store.saveIncoming(checkpoint)
        return IncomingResumeSession(handle, checkpoint, prefix)
    }

    private suspend fun openRestart(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): IncomingResumeSession {
        val beforeLease = requireCurrent(offer, expectedStatus)
        val current = refreshLease(beforeLease.transferId)
        requireExpectedStatus(current, offer, expectedStatus)
        files.delete(current.location.toStoredLocation())
        val handle = files.create(offer.transferId, offer.fileName, offer.mimeType)
        val prefix = emptyPrefix()
        val checkpoint = newCheckpoint(offer, handle, prefix, createdAt = current.createdAt)
        store.saveIncoming(checkpoint)
        return IncomingResumeSession(handle, checkpoint, prefix)
    }

    private suspend fun openResume(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): IncomingResumeSession {
        if (expectedStatus.state != ResumeState.AVAILABLE) {
            throw ResumeValidationException("Checkpoint is not resumable")
        }
        val beforeLease = requireCurrent(offer, expectedStatus)
        val current = refreshLease(beforeLease.transferId)
        requireExpectedStatus(current, offer, expectedStatus)
        val location = current.location.toStoredLocation()
        val handle = files.reopen(location, current.displayName)
            ?: throw ResumeValidationException("Partial file is missing")
        try {
            val length = handle.length()
            if (length < current.confirmedBytes) {
                throw ResumeValidationException("Partial file is shorter than committed prefix")
            }
            if (length > current.confirmedBytes) {
                handle.truncate(current.confirmedBytes)
                handle.force()
            }
            val input = files.openInput(location)
                ?: throw ResumeValidationException("Partial file cannot be read")
            val prefix = input.use {
                try {
                    PrefixDigestScanner.scan(it, current.confirmedBytes, current.chunkSize)
                } catch (error: IOException) {
                    throw ResumeValidationException("Partial file prefix cannot be read", error)
                }
            }
            if (!validatePrefix(expectedStatus, prefix)) {
                throw ResumeValidationException("Partial file prefix does not match checkpoint")
            }
            return IncomingResumeSession(handle, current, prefix)
        } catch (error: Throwable) {
            runCatching { handle.close() }
            throw error
        }
    }

    private suspend fun requireCurrent(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): IncomingCheckpoint {
        val current = store.findIncoming(offer.transferId)
            ?: throw CheckpointConflictException("Incoming checkpoint no longer exists")
        requireExpectedStatus(current, offer, expectedStatus)
        return current
    }

    private fun requireExpectedStatus(
        checkpoint: IncomingCheckpoint,
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ) {
        val actual = if (checkpoint.isResumableFor(offer)) {
            checkpoint.toStatus(ResumeState.AVAILABLE)
        } else {
            ResumeStatus(ResumeState.INVALID)
        }
        if (!actual.sameValue(expectedStatus)) {
            throw CheckpointConflictException("Incoming checkpoint changed during negotiation")
        }
    }

    private suspend fun refreshLease(transferId: String): IncomingCheckpoint {
        val now = clock()
        val expiresAt = now + RETENTION_MILLIS
        if (!store.updateIncomingExpiry(transferId, now, expiresAt)) {
            throw CheckpointConflictException("Incoming checkpoint is being cleaned up")
        }
        val refreshed = store.findIncoming(transferId)
            ?: throw CheckpointConflictException("Incoming checkpoint disappeared during lease refresh")
        return refreshed.copy(updatedAt = now, expiresAt = expiresAt)
    }

    private fun newCheckpoint(
        offer: TransferOffer,
        handle: ResumableFileHandle,
        prefix: PrefixDigest,
        createdAt: Long = clock()
    ): IncomingCheckpoint {
        val now = clock()
        return IncomingCheckpoint(
            transferId = offer.transferId,
            senderDeviceId = offer.senderDeviceId,
            fileName = offer.fileName,
            displayName = handle.displayName,
            mimeType = offer.mimeType,
            fileSize = offer.fileSize,
            chunkSize = offer.chunkSize,
            confirmedBytes = prefix.scannedBytes,
            nextChunkIndex = prefix.nextChunkIndex,
            chainDigest = prefix.chainDigest.copyOf(),
            lastChunkHash = prefix.lastChunkHash.copyOf(),
            storageKind = handle.location.kind,
            storageValue = handle.location.value,
            createdAt = createdAt,
            updatedAt = now,
            expiresAt = now + RETENTION_MILLIS
        )
    }

    private fun emptyPrefix(): PrefixDigest = PrefixDigestScanner.scan(
        input = ByteArray(0).inputStream(),
        bytes = 0,
        chunkSize = TransferProtocol.CHUNK_SIZE
    )

    private fun OutgoingResumeLink.matches(source: SendFileSource): Boolean =
        fileName == source.displayName &&
            mimeType == source.mimeType &&
            fileSize == source.length &&
            lastModified == source.lastModified &&
            chunkSize == TransferProtocol.CHUNK_SIZE

    private fun IncomingCheckpoint.matches(offer: TransferOffer): Boolean =
        senderDeviceId == offer.senderDeviceId &&
            fileName == offer.fileName &&
            mimeType == offer.mimeType &&
            fileSize == offer.fileSize &&
            chunkSize == offer.chunkSize

    private fun IncomingCheckpoint.isResumableFor(offer: TransferOffer): Boolean {
        if (!matches(offer) || chainDigest.size != ChunkCodec.DIGEST_SIZE ||
            lastChunkHash.size != ChunkCodec.DIGEST_SIZE || confirmedBytes !in 0..fileSize
        ) return false
        if (confirmedBytes != fileSize && confirmedBytes % chunkSize != 0L) return false
        val expectedIndex = if (confirmedBytes == 0L) {
            0
        } else {
            ((confirmedBytes - 1) / chunkSize + 1).toInt()
        }
        return nextChunkIndex == expectedIndex
    }

    private fun IncomingCheckpoint.toStatus(state: ResumeState): ResumeStatus = ResumeStatus(
        state = state,
        confirmedBytes = confirmedBytes,
        nextChunkIndex = nextChunkIndex,
        chainDigest = chainDigest.copyOf(),
        lastChunkHash = lastChunkHash.copyOf()
    )

    private fun ResumeStatus.sameValue(other: ResumeStatus): Boolean =
        state == other.state &&
            confirmedBytes == other.confirmedBytes &&
            nextChunkIndex == other.nextChunkIndex &&
            chainDigest.contentEquals(other.chainDigest) &&
            lastChunkHash.contentEquals(other.lastChunkHash)

    private fun ResumeStorageLocation.toStoredLocation() = StoredFileLocation(
        kind = kind,
        value = value
    )

    companion object {
        const val RETENTION_MILLIS = 7L * 24 * 60 * 60 * 1000
    }
}
