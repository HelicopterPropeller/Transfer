package com.example.transfer.service

import java.io.IOException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ResumeStartupGateTest {
    @Test
    fun `recovery then cleanup complete before sender and receiver entry`() = runBlocking {
        val recoveryStarted = CompletableDeferred<Unit>()
        val releaseRecovery = CompletableDeferred<Unit>()
        val events = mutableListOf<String>()
        val gate = ResumeStartupGate(
            scope = this,
            recover = {
                events += "recover"
                recoveryStarted.complete(Unit)
                releaseRecovery.await()
            },
            cleanup = { events += "cleanup" }
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
        assertEquals("cleanup", events[1])
        assertTrue(events.drop(2).toSet() == setOf("receive", "send"))
    }

    @Test
    fun `recovery failure blocks network entry and propagates`() = runBlocking { supervisorScope {
        val entered = CompletableDeferred<Unit>()
        val gate = ResumeStartupGate(
            scope = this,
            recover = { throw IOException("resume database unavailable") },
            cleanup = { error("cleanup must not run") }
        )
        val receiver = gate.launchWhenReady { entered.complete(Unit) }

        val failure = runCatching { gate.awaitReady() }.exceptionOrNull()
        receiver.join()

        assertEquals("resume database unavailable", failure?.message)
        assertFalse(entered.isCompleted)
    } }
}
