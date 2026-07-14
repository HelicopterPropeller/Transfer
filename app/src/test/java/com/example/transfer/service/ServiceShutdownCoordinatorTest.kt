package com.example.transfer.service

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ServiceShutdownCoordinatorTest {
    @Test
    fun `shutdown before gate release prevents later start`() = runBlocking {
        val owner = Job()
        val scope = CoroutineScope(Dispatchers.Default + owner)
        val initializationStarted = CompletableDeferred<Unit>()
        val releaseInitialization = CompletableDeferred<Unit>()
        val startCalled = CompletableDeferred<Unit>()
        val gate = HistoryStartupGate(scope) {
            initializationStarted.complete(Unit)
            releaseInitialization.await()
        }
        val startupJob = gate.launchWhenReady { startCalled.complete(Unit) }
        initializationStarted.await()
        var stopCalls = 0

        ServiceShutdownCoordinator().shutdown(
            cancelOutgoing = {},
            preventNewStarts = {
                startupJob.cancel()
                owner.cancel()
            },
            stopResources = {
                assertFalse(scope.isActive)
                stopCalls++
            }
        )
        releaseInitialization.complete(Unit)
        startupJob.join()

        assertFalse(startCalled.isCompleted)
        assertEquals(1, stopCalls)
    }

    @Test
    fun `started action is followed by inactive owner and one final stop`() = runBlocking {
        val owner = Job()
        val scope = CoroutineScope(Dispatchers.Default + owner)
        val actionStarted = CompletableDeferred<Unit>()
        var socketOpen = false
        var stopCalls = 0
        val order = mutableListOf<String>()
        val startupJob = HistoryStartupGate(scope) {}
            .launchWhenReady {
                socketOpen = true
                actionStarted.complete(Unit)
                awaitCancellation()
            }
        actionStarted.await()
        val coordinator = ServiceShutdownCoordinator()
        val shutdown = {
            coordinator.shutdown(
                cancelOutgoing = { order += "outgoing" },
                preventNewStarts = {
                    order += "prevent"
                    startupJob.cancel()
                    owner.cancel()
                },
                stopResources = {
                    order += "stop"
                    assertFalse(scope.isActive)
                    assertTrue(socketOpen)
                    socketOpen = false
                    stopCalls++
                }
            )
        }

        shutdown()
        shutdown()
        startupJob.join()

        assertEquals(listOf("outgoing", "prevent", "stop"), order)
        assertEquals(1, stopCalls)
        assertFalse(socketOpen)
    }
}
