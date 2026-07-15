package com.example.transfer.resume

import com.example.transfer.storage.ResumableIncomingFileStore
import com.example.transfer.storage.StoredFileLocation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
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
        var deletionError: Exception? = null
        try {
            val expired = store.claimExpiredIncoming(
                now = now,
                staleClaimBefore = now - DAY_MILLIS,
                token = token
            )
            expired.forEach { checkpoint ->
                val locations = listOfNotNull(checkpoint.location, checkpoint.retiredLocation)
                locations.forEach { location ->
                    try {
                        files.delete(
                            StoredFileLocation(
                                kind = location.kind,
                                value = location.value
                            )
                        )
                    } catch (error: CancellationException) {
                        throw error
                    } catch (error: Exception) {
                        if (deletionError == null) {
                            deletionError = error
                        } else {
                            deletionError?.addSuppressed(error)
                        }
                    }
                }
            }
            deletionError?.let { throw it }
            store.deleteClaimedIncoming(token)
            store.deleteExpiredOutgoing(now - RETENTION_MILLIS)
            saveLastRun(now)
            return expired.size
        } catch (error: CancellationException) {
            releaseAfterFailure(token, error)
            throw error
        } catch (error: Exception) {
            releaseAfterFailure(token, error)
            throw error
        }
    }

    private suspend fun releaseAfterFailure(token: String, error: Throwable) {
        try {
            withContext(NonCancellable) { store.releaseClaimedIncoming(token) }
        } catch (releaseError: Exception) {
            error.addSuppressed(releaseError)
        }
    }

    companion object {
        const val DAY_MILLIS = 24L * 60 * 60 * 1000
        const val RETENTION_MILLIS = 7L * DAY_MILLIS
    }
}
