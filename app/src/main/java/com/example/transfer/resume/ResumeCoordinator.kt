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
    val prefix: PrefixDigest,
    val sessionToken: String,
    val generation: Long = checkpoint.generation
)

class ResumeValidationException(message: String, cause: Throwable? = null) : IOException(message, cause)

class CheckpointConflictException(message: String) : IOException(message)

class ResumeCoordinator(
    private val store: ResumeStore,
    private val files: ResumableIncomingFileStore,
    private val clock: () -> Long = System::currentTimeMillis,
    private val newTransferId: () -> String = { UUID.randomUUID().toString() },
    private val newSessionToken: () -> String = { UUID.randomUUID().toString() }
) {
    suspend fun prepareOutgoing(
        source: SendFileSource,
        peerDeviceId: String,
        senderDeviceId: String
    ): PreparedTransfer {
        val sourceUri = requireNotNull(source.sourceUri) { "A stable source URI is required for resume" }
        val now = clock()
        val saved = store.resolveOutgoing(
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
            )
        )
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
        validateChunkTransition(session.checkpoint, confirmedBytes, nextChunkIndex, chainDigest, lastChunkHash)
        session.handle.force()
        val now = clock()
        val expiresAt = now + RETENTION_MILLIS
        val committed = store.commitOwnedIncomingChunk(
            expected = session.checkpoint,
            token = session.sessionToken,
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
        requireOwned(session)
        session.checkpoint.retiredLocation?.let { files.delete(it.toStoredLocation()) }
        val published = files.publish(session.handle)
        if (!store.deleteOwnedIncoming(session.checkpoint, session.sessionToken)) {
            throw CheckpointConflictException("Incoming session lost ownership during completion")
        }
        return published
    }

    suspend fun releaseIncoming(session: IncomingResumeSession) {
        session.handle.close()
        if (!store.releaseIncomingSession(session.checkpoint, session.sessionToken)) {
            throw CheckpointConflictException("Incoming session no longer owns checkpoint")
        }
    }

    suspend fun clearStaleSessionClaims(staleClaimBefore: Long): Int =
        store.clearStaleIncomingSessions(staleClaimBefore)

    suspend fun completeOutgoing(transferId: String) {
        store.deleteOutgoing(transferId)
    }

    private suspend fun openNew(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): IncomingResumeSession {
        if (expectedStatus.state != ResumeState.NONE) {
            throw CheckpointConflictException("Incoming checkpoint already exists")
        }
        val token = newSessionToken()
        val handle = files.create(stagingId(offer.transferId, token, 1), offer.fileName, offer.mimeType)
        val prefix = emptyPrefix()
        val checkpoint = newCheckpoint(offer, handle, prefix, generation = 1, token = token)
        try {
            if (!store.insertIncoming(checkpoint)) {
                throw CheckpointConflictException("Incoming checkpoint already exists")
            }
        } catch (error: Exception) {
            cleanupStaged(handle, error)
            throw error
        }
        return IncomingResumeSession(handle, checkpoint, prefix, token)
    }

    private suspend fun openRestart(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): IncomingResumeSession {
        val acquired = acquireCurrent(offer, expectedStatus)
        val current = recoverRetired(acquired.first, acquired.second, requireSuccess = true)
        val token = acquired.second
        val handle = try {
            files.create(stagingId(offer.transferId, token, current.generation + 1), offer.fileName, offer.mimeType)
        } catch (error: Exception) {
            store.releaseIncomingSession(current, token)
            throw error
        }
        val prefix = emptyPrefix()
        val replacement = newCheckpoint(
            offer, handle, prefix, createdAt = current.createdAt,
            generation = current.generation + 1, token = token,
            retired = current.location
        )
        try {
            if (!store.replaceIncomingForRestart(current, replacement, token)) {
                throw CheckpointConflictException("Incoming checkpoint changed during restart")
            }
        } catch (error: Exception) {
            cleanupStaged(handle, error)
            store.releaseIncomingSession(current, token)
            throw error
        }
        var active = replacement
        try {
            files.delete(current.location.toStoredLocation())
            if (store.clearRetiredIncoming(active, token)) {
                active = active.copy(retiredStorageKind = null, retiredStorageValue = null)
            }
        } catch (_: Exception) {
            // Retired location remains durable and is retried by later open/cleanup.
        }
        return IncomingResumeSession(handle, active, prefix, token)
    }

    private suspend fun openResume(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): IncomingResumeSession {
        if (expectedStatus.state != ResumeState.AVAILABLE) {
            throw ResumeValidationException("Checkpoint is not resumable")
        }
        val acquired = acquireCurrent(offer, expectedStatus)
        val token = acquired.second
        var current = recoverRetired(acquired.first, token, requireSuccess = false)
        val location = current.location.toStoredLocation()
        val handle = files.reopen(location, current.displayName)
        if (handle == null) {
            store.releaseIncomingSession(current, token)
            throw ResumeValidationException("Partial file is missing")
        }
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
            return IncomingResumeSession(handle, current, prefix, token)
        } catch (error: Exception) {
            runCatching { handle.close() }
            store.releaseIncomingSession(current, token)
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

    private suspend fun acquireCurrent(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): Pair<IncomingCheckpoint, String> {
        val expected = requireCurrent(offer, expectedStatus)
        val token = newSessionToken()
        val now = clock()
        val expiresAt = now + RETENTION_MILLIS
        val acquired = store.acquireIncomingSession(expected, token, now, expiresAt)
            ?: throw CheckpointConflictException("Incoming checkpoint is already owned or being cleaned up")
        try {
            requireExpectedStatus(acquired, offer, expectedStatus)
        } catch (error: Exception) {
            store.releaseIncomingSession(acquired, token)
            throw error
        }
        return acquired to token
    }

    private fun newCheckpoint(
        offer: TransferOffer,
        handle: ResumableFileHandle,
        prefix: PrefixDigest,
        createdAt: Long = clock(),
        generation: Long,
        token: String,
        retired: ResumeStorageLocation? = null
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
            expiresAt = now + RETENTION_MILLIS,
            generation = generation,
            sessionToken = token,
            sessionClaimedAt = now,
            retiredStorageKind = retired?.kind,
            retiredStorageValue = retired?.value
        )
    }

    private suspend fun recoverRetired(
        checkpoint: IncomingCheckpoint,
        token: String,
        requireSuccess: Boolean
    ): IncomingCheckpoint {
        val retired = checkpoint.retiredLocation ?: return checkpoint
        return try {
            files.delete(retired.toStoredLocation())
            if (!store.clearRetiredIncoming(checkpoint, token)) {
                throw CheckpointConflictException("Retired location changed during recovery")
            }
            checkpoint.copy(retiredStorageKind = null, retiredStorageValue = null)
        } catch (error: Exception) {
            if (requireSuccess) {
                store.releaseIncomingSession(checkpoint, token)
                throw error
            }
            checkpoint
        }
    }

    private suspend fun requireOwned(session: IncomingResumeSession) {
        val current = store.findIncoming(session.checkpoint.transferId)
        if (current == null || current.sessionToken != session.sessionToken ||
            current.generation != session.generation || current.storageKind != session.checkpoint.storageKind ||
            current.storageValue != session.checkpoint.storageValue ||
            current.nextChunkIndex != session.checkpoint.nextChunkIndex
        ) throw CheckpointConflictException("Incoming session no longer owns checkpoint")
    }

    private fun validateChunkTransition(
        old: IncomingCheckpoint,
        confirmedBytes: Long,
        nextChunkIndex: Int,
        chainDigest: ByteArray,
        lastChunkHash: ByteArray
    ) {
        val delta = confirmedBytes - old.confirmedBytes
        val valid = nextChunkIndex == old.nextChunkIndex + 1 && delta in 1..old.chunkSize.toLong() &&
            confirmedBytes <= old.fileSize &&
            (confirmedBytes == old.fileSize || delta == old.chunkSize.toLong()) &&
            chainDigest.size == ChunkCodec.DIGEST_SIZE && lastChunkHash.size == ChunkCodec.DIGEST_SIZE
        if (!valid) throw ResumeValidationException("Invalid incoming chunk transition")
    }

    private suspend fun cleanupStaged(handle: ResumableFileHandle, original: Exception) {
        runCatching { handle.close() }.exceptionOrNull()?.let(original::addSuppressed)
        runCatching { files.delete(handle.location) }.exceptionOrNull()?.let(original::addSuppressed)
    }

    private fun stagingId(transferId: String, token: String, generation: Long) =
        "$transferId-$generation-$token"

    private fun emptyPrefix(): PrefixDigest = PrefixDigestScanner.scan(
        input = ByteArray(0).inputStream(),
        bytes = 0,
        chunkSize = TransferProtocol.CHUNK_SIZE
    )

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
