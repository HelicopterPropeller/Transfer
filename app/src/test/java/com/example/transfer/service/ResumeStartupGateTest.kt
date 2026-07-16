package com.example.transfer.service

import java.io.IOException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ResumeStartupGateTest {
    @Test
    fun `recover failure is reported once without reaching coroutine exception handler`() = runBlocking {
        assertLaunchFailureIsHandled(failCleanup = false)
    }

    @Test
    fun `recovery completes before sender and receiver entry`() = runBlocking {
        val recoveryStarted = CompletableDeferred<Unit>()
        val releaseRecovery = CompletableDeferred<Unit>()
        val events = mutableListOf<String>()
        val gate = ResumeStartupGate(
            scope = this,
            recover = {
                events += "recover"
                recoveryStarted.complete(Unit)
                releaseRecovery.await()
            }
        )
        val receiver = gate.launchWhenReady { events += "receive" }
        val sender = async {
            gate.awaitReady()
            events += "send"
        }

        recoveryStarted.await()
        assertEquals(listOf("recover"), events)
        assertFalse(receiver.isCompleted)
        assertFalse(sender.isCompleted)
        releaseRecovery.complete(Unit)
        receiver.join()
        sender.await()

        assertEquals("recover", events[0])
        assertTrue(events.drop(1).toSet() == setOf("receive", "send"))
    }

    @Test
    fun `recovery failure blocks network entry and propagates`() = runBlocking { supervisorScope {
        val entered = CompletableDeferred<Unit>()
        val gate = ResumeStartupGate(
            scope = this,
            recover = { throw IOException("resume database unavailable") }
        )
        val receiver = gate.launchWhenReady { entered.complete(Unit) }

        val failure = runCatching { gate.awaitReady() }.exceptionOrNull()
        receiver.join()

        assertEquals("resume database unavailable", failure?.message)
        assertFalse(entered.isCompleted)
    } }

    private suspend fun assertLaunchFailureIsHandled(failCleanup: Boolean = false) {
        val unhandled = mutableListOf<Throwable>()
        val handler = CoroutineExceptionHandler { _, error -> unhandled += error }
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Unconfined + handler)
        val failures = mutableListOf<Throwable>()
        var serverStarted = false
        val expectedMessage = "recover failed"
        val gate = ResumeStartupGate(
            scope = scope,
            recover = { throw IOException(expectedMessage) }
        )
        try {
            gate.launchWhenReady(
                onFailure = { failures += it },
                action = { serverStarted = true }
            ).join()

            assertEquals(listOf(expectedMessage), failures.map { it.message })
            assertFalse(serverStarted)
            assertTrue(unhandled.isEmpty())
            assertEquals(expectedMessage, runCatching { gate.awaitReady() }.exceptionOrNull()?.message)
        } finally {
            scope.cancel()
        }
    }
}
