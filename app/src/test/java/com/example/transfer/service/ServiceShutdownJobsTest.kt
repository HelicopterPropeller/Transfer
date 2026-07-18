package com.example.transfer.service

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ServiceShutdownJobsTest {
    @Test
    fun `cancel and join waits for non cancellable cleanup`() = runBlocking {
        var cleanupFinished = false
        val started = CompletableDeferred<Unit>()
        val worker = launch(Dispatchers.Default) {
            try {
                started.complete(Unit)
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    delay(25)
                    cleanupFinished = true
                }
            }
        }
        started.await()

        ServiceShutdownJobs.cancelAndJoin(listOf(worker))

        assertTrue(worker.isCompleted)
        assertTrue(cleanupFinished)
    }

    @Test
    fun `cancel and join skips the current job`() = runBlocking {
        val currentJob = currentCoroutineContext()[Job]!!
        val otherJob = launch(Dispatchers.Default) { awaitCancellation() }

        ServiceShutdownJobs.cancelAndJoin(listOf(currentJob, otherJob), currentJob)

        assertTrue(currentJob.isActive)
        assertFalse(currentJob.isCancelled)
        assertTrue(otherJob.isCompleted)
    }
}
