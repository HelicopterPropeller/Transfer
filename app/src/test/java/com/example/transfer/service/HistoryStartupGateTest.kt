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

    @Test
    fun `start action waits for initialization release then runs`() = runBlocking {
        val initializationStarted = CompletableDeferred<Unit>()
        val releaseInitialization = CompletableDeferred<Unit>()
        val startCalled = CompletableDeferred<Unit>()
        val gate = HistoryStartupGate(this) {
            initializationStarted.complete(Unit)
            releaseInitialization.await()
        }

        val startJob = gate.launchWhenReady { startCalled.complete(Unit) }
        initializationStarted.await()
        assertFalse(startCalled.isCompleted)

        releaseInitialization.complete(Unit)
        startJob.join()
        assertTrue(startCalled.isCompleted)
    }

    @Test
    fun `ordinary initialization failure still runs start action`() = runBlocking {
        val startCalled = CompletableDeferred<Unit>()
        val gate = HistoryStartupGate(this) {
            throw IOException("history unavailable")
        }

        gate.launchWhenReady { startCalled.complete(Unit) }.join()

        assertTrue(startCalled.isCompleted)
    }

    @Test
    fun `owner cancellation prevents pending start action`() = runBlocking {
        val owner = Job()
        val scope = CoroutineScope(Dispatchers.Default + owner)
        val initializationStarted = CompletableDeferred<Unit>()
        val startCalled = CompletableDeferred<Unit>()
        val gate = HistoryStartupGate(scope) {
            initializationStarted.complete(Unit)
            awaitCancellation()
        }
        val startJob = gate.launchWhenReady { startCalled.complete(Unit) }
        initializationStarted.await()

        owner.cancel()
        startJob.join()

        assertFalse(startCalled.isCompleted)
    }
}
