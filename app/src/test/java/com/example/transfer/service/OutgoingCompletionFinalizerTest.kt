package com.example.transfer.service

import java.io.IOException
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class OutgoingCompletionFinalizerTest {
    @Test
    fun `network success retries cleanup and reports the final persistence failure`() = runBlocking {
        var cleanupAttempts = 0
        var reported: Throwable? = null
        val networkResult = Result.success(Unit)

        val result = finalizeOutgoingNetworkResult(
            networkResult,
            onCleanupFailure = { reported = it }
        ) {
                cleanupAttempts++
                throw IOException("database locked")
            }

        assertTrue(result.isSuccess)
        assertEquals(3, cleanupAttempts)
        assertEquals("database locked", reported?.message)
    }

    @Test
    fun `transient cleanup failure succeeds on retry without reporting`() = runBlocking {
        var cleanupAttempts = 0
        var reported = false

        val result = finalizeOutgoingNetworkResult(
            Result.success(Unit),
            onCleanupFailure = { reported = true }
        ) {
            cleanupAttempts++
            if (cleanupAttempts == 1) throw IOException("database locked")
        }

        assertTrue(result.isSuccess)
        assertEquals(2, cleanupAttempts)
        assertTrue(!reported)
    }

    @Test
    fun `network failure does not delete retry metadata`() = runBlocking {
        var cleanupAttempts = 0
        val networkResult = Result.failure<Unit>(IOException("connection lost"))

        val result = finalizeOutgoingNetworkResult(networkResult) { cleanupAttempts++ }

        assertTrue(result.isFailure)
        assertEquals(0, cleanupAttempts)
    }
}
