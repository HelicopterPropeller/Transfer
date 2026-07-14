package com.example.transfer.service

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.IOException
import java.util.concurrent.CancellationException

class HistoryStartupGateTest {
    @Test
    fun `send entry waits until history initialization completes`() = runBlocking {
        val initializationStarted = CompletableDeferred<Unit>()
        val releaseInitialization = CompletableDeferred<Unit>()
        val sendEntered = CompletableDeferred<Unit>()
        val gate = HistoryStartupGate(this) {
            initializationStarted.complete(Unit)
            releaseInitialization.await()
        }
        val send = async {
            gate.awaitReady()
            sendEntered.complete(Unit)
        }

        initializationStarted.await()
        assertFalse(sendEntered.isCompleted)
        releaseInitialization.complete(Unit)
        send.await()

        assertTrue(sendEntered.isCompleted)
    }

    @Test
    fun `ordinary initialization failure releases send entry`() = runBlocking {
        val gate = HistoryStartupGate(this) {
            throw IOException("history unavailable")
        }

        gate.awaitReady()
    }

    @Test
    fun `owner cancellation cancels initialization wait`() = runBlocking {
        val owner = Job()
        val scope = CoroutineScope(Dispatchers.Default + owner)
        val initializationStarted = CompletableDeferred<Unit>()
        val gate = HistoryStartupGate(scope) {
            initializationStarted.complete(Unit)
            awaitCancellation()
        }
        initializationStarted.await()
        val waiter = async { gate.awaitReady() }

        owner.cancel()
        try {
            waiter.await()
            fail("Expected cancellation")
        } catch (_: CancellationException) {
            // Scope shutdown is expected to cancel the gate and its waiters.
        } finally {
            owner.cancelAndJoin()
        }
    }
}
