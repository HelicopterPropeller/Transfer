package com.example.transfer.resume

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.DigestChain
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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

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
        val expected = checkpoint.toStatus(ResumeState.AVAILABLE)
        return try {
            val input = files.openInput(checkpoint.location.toStoredLocation())
                ?: return ResumeStatus(ResumeState.INVALID)
            val prefix = input.use {
                PrefixDigestScanner.scan(it, checkpoint.confirmedBytes, checkpoint.chunkSize)
            }
            if (validatePrefix(expected, prefix)) expected else ResumeStatus(ResumeState.INVALID)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            ResumeStatus(ResumeState.INVALID)
        }
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
        val active = heartbeatIncoming(session)
        validateChunkTransition(active.checkpoint, confirmedBytes, nextChunkIndex, chainDigest, lastChunkHash)
        active.handle.force()
        val now = clock()
        val expiresAt = now + RETENTION_MILLIS
        val committed = store.commitOwnedIncomingChunk(
            expected = active.checkpoint,
            token = active.sessionToken,
            confirmedBytes = confirmedBytes,
            nextChunkIndex = nextChunkIndex,
            chainDigest = chainDigest,
            lastChunkHash = lastChunkHash,
            updatedAt = now,
            expiresAt = expiresAt
        )
        if (!committed) throw CheckpointConflictException("Incoming checkpoint changed during commit")

        return active.copy(
            checkpoint = active.checkpoint.copy(
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
                wholeDigest = active.prefix.wholeDigest
            )
        )
    }

    suspend fun completeIncoming(session: IncomingResumeSession): String? {
        var active = heartbeatIncoming(session)
        active = active.copy(
            checkpoint = recoverRetired(active.checkpoint, active.sessionToken, requireSuccess = true)
        )
        val now = clock()
        val expiresAt = now + RETENTION_MILLIS
        if (!store.beginIncomingCompletion(active.checkpoint, active.sessionToken, now, expiresAt)) {
            throw CheckpointConflictException("Incoming session lost ownership before completion")
        }
        val completing = active.checkpoint.copy(
            operationState = IncomingOperationState.COMPLETING,
            sessionClaimedAt = now,
            updatedAt = now,
            expiresAt = expiresAt
        )
        val published = files.publish(active.handle)
        if (!store.deleteCompletingIncoming(completing)) {
            throw CheckpointConflictException("Completed checkpoint changed before final cleanup")
        }
        return published
    }

    suspend fun heartbeatIncoming(session: IncomingResumeSession): IncomingResumeSession {
        val now = clock()
        val expiresAt = now + RETENTION_MILLIS
        if (!store.heartbeatIncomingSession(session.checkpoint, session.sessionToken, now, expiresAt)) {
            throw CheckpointConflictException("Incoming session no longer owns checkpoint")
        }
        return session.copy(
            checkpoint = session.checkpoint.copy(
                operationState = IncomingOperationState.ACTIVE,
                sessionClaimedAt = now,
                updatedAt = now,
                expiresAt = expiresAt
            )
        )
    }

    suspend fun releaseIncoming(session: IncomingResumeSession) {
        var failure: Throwable? = null
        try {
            session.handle.close()
        } catch (error: Throwable) {
            failure = error
        }
        try {
            withContext(NonCancellable) {
                if (!store.releaseIncomingSession(session.checkpoint, session.sessionToken)) {
                    throw CheckpointConflictException("Incoming session no longer owns checkpoint")
                }
            }
        } catch (error: Throwable) {
            if (failure == null) failure = error else failure.addSuppressed(error)
        }
        failure?.let { throw it }
    }

    suspend fun clearStaleSessionClaims(staleClaimBefore: Long): Int =
        store.clearStaleIncomingSessions(staleClaimBefore)

    /** Repairs journaled create/switch windows and durable completion after a process restart. */
    suspend fun recoverInterruptedState(staleClaimBefore: Long): Int {
        var recovered = 0
        store.findStagingJournals().forEach { journal ->
            val staged = files.findStaging(journal.stagingId)
            val current = store.findIncoming(journal.transferId)
            val switched = staged != null && current != null &&
                current.storageKind == staged.kind && current.storageValue == staged.value
            if (switched) {
                recoverRetiredAfterProcessDeath(current!!)
            } else if (staged != null) {
                files.delete(staged)
            }
            if (store.deleteStagingJournal(journal)) recovered++
        }

        store.findCompletingIncoming().forEach { checkpoint ->
            if (!recoverRetiredAfterProcessDeath(checkpoint)) return@forEach
            try {
                val alreadyPublished = files.recoverPublished(
                    checkpoint.location.toStoredLocation(), checkpoint.displayName
                )
                if (alreadyPublished == null) {
                    val handle = files.reopen(
                        checkpoint.location.toStoredLocation(), checkpoint.displayName
                    ) ?: return@forEach
                    files.publish(handle)
                }
                if (store.deleteCompletingIncoming(checkpoint)) recovered++
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                // COMPLETING is intentionally durable; the next startup retries it.
            }
        }
        recovered += store.clearStaleIncomingSessions(staleClaimBefore)
        return recovered
    }

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
        val journal = IncomingStagingJournal(offer.transferId, stagingId(offer.transferId, 1), clock())
        if (!store.insertStagingJournal(journal)) {
            throw CheckpointConflictException("Incoming staging operation already exists")
        }
        val handle = try {
            files.create(journal.stagingId, offer.fileName, offer.mimeType)
        } catch (error: Exception) {
            cleanupJournaledStaging(journal, error)
            throw error
        }
        val prefix = emptyPrefix()
        val checkpoint = newCheckpoint(offer, handle, prefix, generation = 1, token = token)
        try {
            if (!store.insertIncoming(checkpoint)) {
                throw CheckpointConflictException("Incoming checkpoint already exists")
            }
        } catch (error: Exception) {
            val removed = cleanupStaged(handle, error)
            deleteJournalAfterCleanup(journal, removed, error)
            throw error
        }
        deleteJournalAfterSwitch(journal)
        return IncomingResumeSession(handle, checkpoint, prefix, token)
    }

    private suspend fun openRestart(
        offer: TransferOffer,
        expectedStatus: ResumeStatus
    ): IncomingResumeSession {
        val acquired = acquireCurrent(offer, expectedStatus)
        val current = recoverRetired(acquired.first, acquired.second, requireSuccess = true)
        val token = acquired.second
        val journal = IncomingStagingJournal(
            offer.transferId,
            stagingId(offer.transferId, current.generation + 1),
            clock()
        )
        try {
            if (!store.insertStagingJournal(journal)) {
                throw CheckpointConflictException("Incoming staging operation already exists")
            }
        } catch (error: Exception) {
            releaseSessionAfterFailure(current, token, error)
            throw error
        }
        val handle = try {
            files.create(journal.stagingId, offer.fileName, offer.mimeType)
        } catch (error: Exception) {
            cleanupJournaledStaging(journal, error)
            releaseSessionAfterFailure(current, token, error)
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
            val removed = cleanupStaged(handle, error)
            deleteJournalAfterCleanup(journal, removed, error)
            releaseSessionAfterFailure(current, token, error)
            throw error
        }
        deleteJournalAfterSwitch(journal)
        var active = replacement
        try {
            files.delete(current.location.toStoredLocation())
            if (store.clearRetiredIncoming(active, token)) {
                active = active.copy(retiredStorageKind = null, retiredStorageValue = null)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
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
            var activeSession = IncomingResumeSession(handle, current, emptyPrefix(), token)
            activeSession = heartbeatIncoming(activeSession)
            current = activeSession.checkpoint
            val length = handle.length()
            if (length < current.confirmedBytes) {
                throw ResumeValidationException("Partial file is shorter than committed prefix")
            }
            if (length > current.confirmedBytes) {
                handle.truncate(current.confirmedBytes)
                handle.force()
                current = heartbeatIncoming(activeSession.copy(checkpoint = current)).checkpoint
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
            val refreshed = heartbeatIncoming(
                activeSession.copy(checkpoint = current, prefix = prefix)
            )
            return refreshed.copy(prefix = prefix)
        } catch (cancelled: CancellationException) {
            runCatching { handle.close() }
            withContext(NonCancellable) { store.releaseIncomingSession(current, token) }
            throw cancelled
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
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) { store.releaseIncomingSession(acquired, token) }
            throw cancelled
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
            operationState = IncomingOperationState.ACTIVE,
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
        } catch (cancelled: CancellationException) {
            if (requireSuccess) {
                withContext(NonCancellable) {
                    store.releaseIncomingSession(checkpoint, token)
                }
            }
            throw cancelled
        } catch (error: Exception) {
            if (requireSuccess) {
                store.releaseIncomingSession(checkpoint, token)
                throw error
            }
            checkpoint
        }
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
        val expectedChain = if (valid) {
            DigestChain.next(old.chainDigest, old.nextChunkIndex, delta.toInt(), lastChunkHash)
        } else null
        if (!valid || !chainDigest.contentEquals(expectedChain!!)) {
            throw ResumeValidationException("Invalid incoming chunk transition")
        }
    }

    private suspend fun cleanupStaged(handle: ResumableFileHandle, original: Throwable): Boolean {
        runCatching { handle.close() }.exceptionOrNull()?.let(original::addSuppressed)
        return try {
            files.delete(handle.location)
            true
        } catch (error: Throwable) {
            original.addSuppressed(error)
            false
        }
    }

    private suspend fun deleteJournalAfterCleanup(
        journal: IncomingStagingJournal,
        stagedAbsentOrDeleted: Boolean,
        original: Throwable
    ) {
        if (!stagedAbsentOrDeleted) return
        try {
            store.deleteStagingJournal(journal)
        } catch (error: Throwable) {
            original.addSuppressed(error)
        }
    }

    private suspend fun releaseSessionAfterFailure(
        checkpoint: IncomingCheckpoint,
        token: String,
        original: Throwable
    ) {
        try {
            withContext(NonCancellable) {
                if (!store.releaseIncomingSession(checkpoint, token)) {
                    throw CheckpointConflictException("Incoming session no longer owns checkpoint")
                }
            }
        } catch (error: Throwable) {
            original.addSuppressed(error)
        }
    }

    private suspend fun deleteJournalAfterSwitch(journal: IncomingStagingJournal) {
        try {
            store.deleteStagingJournal(journal)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            // The checkpoint/location is authoritative; startup reconciles the leftover journal.
        }
    }

    private suspend fun cleanupJournaledStaging(
        journal: IncomingStagingJournal,
        original: Exception
    ) {
        val staged = try {
            files.findStaging(journal.stagingId)
        } catch (error: Throwable) {
            original.addSuppressed(error)
            return
        }
        val removed = if (staged == null) {
            true
        } else {
            try {
                files.delete(staged)
                true
            } catch (error: Throwable) {
                original.addSuppressed(error)
                false
            }
        }
        deleteJournalAfterCleanup(journal, removed, original)
    }

    private suspend fun recoverRetiredAfterProcessDeath(checkpoint: IncomingCheckpoint): Boolean {
        val retired = checkpoint.retiredLocation ?: return true
        return try {
            files.delete(retired.toStoredLocation())
            store.clearRetiredIncomingForRecovery(checkpoint)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            false
        }
    }

    private fun stagingId(transferId: String, generation: Long) = "$transferId-$generation"

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
