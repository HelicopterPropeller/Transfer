package com.example.transfer.service

import java.io.IOException
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class OutgoingCompletionFinalizerTest {
    @Test
    fun `network success remains success when resume row cleanup fails`() = runBlocking {
        var cleanupAttempts = 0
        val networkResult = Result.success(Unit)

        val result = finalizeOutgoingNetworkResult(networkResult) {
            cleanupAttempts++
            throw IOException("database locked")
        }

        assertTrue(result.isSuccess)
        assertEquals(1, cleanupAttempts)
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
