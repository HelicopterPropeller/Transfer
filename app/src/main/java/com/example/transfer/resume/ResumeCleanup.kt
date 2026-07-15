package com.example.transfer.resume

import com.example.transfer.storage.ResumableIncomingFileStore
import com.example.transfer.storage.StoredFileLocation
import java.util.UUID

class ResumeCleanup(
    private val store: ResumeStore,
    private val files: ResumableIncomingFileStore,
    private val clock: () -> Long,
    private val lastRun: () -> Long,
    private val saveLastRun: (Long) -> Unit
) {
    suspend fun runIfDue(force: Boolean = false): Int {
        val now = clock()
        if (!force && now - lastRun() < DAY_MILLIS) return 0

        val token = UUID.randomUUID().toString()
        val expired = store.claimExpiredIncoming(
            now = now,
            staleClaimBefore = now - DAY_MILLIS,
            token = token
        )
        try {
            var deletionError: Throwable? = null
            expired.forEach { checkpoint ->
                try {
                    files.delete(
                        StoredFileLocation(
                            kind = checkpoint.location.kind,
                            value = checkpoint.location.value
                        )
                    )
                } catch (error: Throwable) {
                    if (deletionError == null) {
                        deletionError = error
                    } else {
                        deletionError.addSuppressed(error)
                    }
                }
            }
            deletionError?.let { throw it }
            store.deleteClaimedIncoming(token)
            store.deleteExpiredOutgoing(now - RETENTION_MILLIS)
            saveLastRun(now)
            return expired.size
        } catch (error: Throwable) {
            runCatching { store.releaseClaimedIncoming(token) }
                .exceptionOrNull()
                ?.let(error::addSuppressed)
            throw error
        }
    }

    companion object {
        const val DAY_MILLIS = 24L * 60 * 60 * 1000
        const val RETENTION_MILLIS = 7L * DAY_MILLIS
    }
}
