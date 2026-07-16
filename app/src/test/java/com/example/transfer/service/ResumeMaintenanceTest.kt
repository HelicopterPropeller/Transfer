package com.example.transfer.service

import java.io.IOException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ResumeMaintenanceTest {
    @Test
    fun `cleanup failure is soft and is retried while service scope lives`() = runBlocking {
        var attempts = 0
        val succeeded = CompletableDeferred<Unit>()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Unconfined)
        val maintenance = ResumeMaintenance(
            scope = scope,
            retryDelayMillis = 1,
            cleanup = {
                attempts++
                if (attempts == 1) throw IOException("storage unavailable")
                succeeded.complete(Unit)
            }
        )
        try {
            withTimeout(2_000) { succeeded.await() }
            assertTrue(attempts >= 2)
        } finally {
            maintenance.cancel()
            scope.cancel()
        }
    }

    @Test
    fun `cancel stops future cleanup attempts`() = runBlocking {
        val first = CompletableDeferred<Unit>()
        var attempts = 0
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Unconfined)
        val maintenance = ResumeMaintenance(
            scope = scope,
            retryDelayMillis = 50,
            cleanup = {
                attempts++
                first.complete(Unit)
            }
        )
        first.await()
        maintenance.cancel()
        val stoppedAt = attempts
        kotlinx.coroutines.delay(100)
        assertEquals(stoppedAt, attempts)
        scope.cancel()
    }
}
