package com.example.transfer.service

import java.util.concurrent.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

internal class ResumeMaintenance(
    scope: CoroutineScope,
    private val retryDelayMillis: Long = DEFAULT_RETRY_DELAY_MILLIS,
    private val cleanup: suspend () -> Unit
) {
    init {
        require(retryDelayMillis > 0)
    }

    private val job: Job = scope.launch {
        while (isActive) {
            try {
                cleanup()
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                // Expiry cleanup is maintenance only. A later tick retries it.
            }
            delay(retryDelayMillis)
        }
    }

    fun cancel() {
        job.cancel()
    }

    private companion object {
        const val DEFAULT_RETRY_DELAY_MILLIS = 60L * 60 * 1000
    }
}
