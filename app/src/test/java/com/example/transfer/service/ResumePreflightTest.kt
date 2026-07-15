package com.example.transfer.service

import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferStartMode
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class ResumePreflightTest {
    @Test
    fun `batch without resumable files starts immediately with safe modes`() {
        val preflight = ResumePreflight<String>()
        val token = requireNotNull(preflight.reserve())

        val result = preflight.finish(token, listOf(
            file("new", ResumeState.NONE),
            file("invalid", ResumeState.INVALID)
        ))

        val ready = result as ResumePreflightResult.Ready
        assertEquals(listOf(TransferStartMode.NEW, TransferStartMode.RESTART), ready.files.map { it.mode })
        assertNull(preflight.prompt)
        assertNotNull(preflight.reserve())
    }

    @Test
    fun `completed receipt starts immediately without prompting or restarting`() {
        val preflight = ResumePreflight<String>()
        val token = requireNotNull(preflight.reserve())

        val result = preflight.finish(token, listOf(file("done", ResumeState.COMPLETED)))

        val ready = result as ResumePreflightResult.Ready
        assertEquals(TransferStartMode.RESUME, ready.files.single().mode)
        assertEquals(ResumeState.COMPLETED, ready.files.single().status.state)
        assertNull(preflight.prompt)
    }

    @Test
    fun `one or many available checkpoints publish one batch prompt`() {
        listOf(1, 3).forEach { availableCount ->
            val preflight = ResumePreflight<String>()
            val token = requireNotNull(preflight.reserve())
            val files = (1..availableCount).map { file("resume-$it", ResumeState.AVAILABLE) } +
                file("new", ResumeState.NONE)

            val result = preflight.finish(token, files)

            val waiting = result as ResumePreflightResult.Waiting
            assertEquals(availableCount, waiting.prompt.resumableFileNames.size)
            assertEquals(files.size, waiting.prompt.fileCount)
            assertEquals(waiting.prompt, preflight.prompt)
        }
    }

    @Test
    fun `waiting prompt does not acquire transfer gate`() {
        var transferGateCalls = 0
        val preflight = ResumePreflight<String>()
        val token = requireNotNull(preflight.reserve())

        preflight.finish(token, listOf(file("resume", ResumeState.AVAILABLE)))

        assertEquals(0, transferGateCalls)
        assertTrue(preflight.hasPendingBatch)
        assertTrue(runIncoming { transferGateCalls++ })
        assertEquals(1, transferGateCalls)
    }

    @Test
    fun `stale confirmation is ignored and cancel clears current batch`() {
        val preflight = ResumePreflight<String>()
        val token = requireNotNull(preflight.reserve())
        val prompt = (preflight.finish(
            token, listOf(file("resume", ResumeState.AVAILABLE))
        ) as ResumePreflightResult.Waiting).prompt

        assertTrue(preflight.confirm(prompt.id + 1, ResumeChoice.RESUME_AVAILABLE) is ResumeConfirmation.Ignored)
        assertEquals(prompt, preflight.prompt)
        assertTrue(preflight.confirm(prompt.id, ResumeChoice.CANCEL) is ResumeConfirmation.Cancelled)
        assertNull(preflight.prompt)
        assertFalse(preflight.hasPendingBatch)
    }

    @Test
    fun `second outgoing preflight cannot replace reserved or waiting batch`() {
        val preflight = ResumePreflight<String>()
        val token = requireNotNull(preflight.reserve())

        assertNull(preflight.reserve())
        val prompt = (preflight.finish(
            token, listOf(file("first", ResumeState.AVAILABLE))
        ) as ResumePreflightResult.Waiting).prompt
        assertNull(preflight.reserve())
        assertEquals(listOf("first"), prompt.resumableFileNames)
    }

    @Test
    fun `resume available preserves per file mapping`() {
        val preflight = waitingBatch()
        val prompt = requireNotNull(preflight.prompt)

        val ready = preflight.confirm(prompt.id, ResumeChoice.RESUME_AVAILABLE) as ResumeConfirmation.Ready

        assertEquals(
            listOf(TransferStartMode.RESUME, TransferStartMode.NEW, TransferStartMode.RESTART),
            ready.files.map { it.mode }
        )
    }

    @Test
    fun `restart all restarts existing checkpoints but keeps new files new`() {
        val preflight = waitingBatch()
        val prompt = requireNotNull(preflight.prompt)

        val ready = preflight.confirm(prompt.id, ResumeChoice.RESTART_ALL) as ResumeConfirmation.Ready

        assertEquals(
            listOf(TransferStartMode.RESTART, TransferStartMode.NEW, TransferStartMode.RESTART),
            ready.files.map { it.mode }
        )
    }

    @Test
    fun `busy race after confirmation leaves no pending batch`() {
        val preflight = waitingBatch()
        val prompt = requireNotNull(preflight.prompt)
        val confirmation = preflight.confirm(
            prompt.id, ResumeChoice.RESUME_AVAILABLE, acquireBusy = { false }
        )

        assertTrue(confirmation is ResumeConfirmation.Busy)
        assertNull(preflight.prompt)
        assertFalse(preflight.hasPendingBatch)
    }

    @Test
    fun `confirm handoff atomically blocks second reservation until busy is visible`() {
        val busy = AtomicBoolean(false)
        val preflight = ResumePreflight<String>()
        val firstToken = requireNotNull(preflight.reserve(busy::get))
        val prompt = (preflight.finish(
            firstToken,
            listOf(file("resume", ResumeState.AVAILABLE)),
            acquireBusy = { busy.compareAndSet(false, true) }
        ) as ResumePreflightResult.Waiting).prompt
        val busyAcquired = CountDownLatch(1)
        val releaseHandoff = CountDownLatch(1)
        var confirmation: ResumeConfirmation<String>? = null
        val first = thread {
            confirmation = preflight.confirm(
                prompt.id,
                ResumeChoice.RESUME_AVAILABLE,
                acquireBusy = {
                    val acquired = busy.compareAndSet(false, true)
                    busyAcquired.countDown()
                    releaseHandoff.await(2, TimeUnit.SECONDS)
                    acquired
                }
            )
        }
        assertTrue(busyAcquired.await(2, TimeUnit.SECONDS))
        var secondToken: Long? = -1
        var secondClientQueries = 0
        val secondStarted = CountDownLatch(1)
        val secondFinished = CountDownLatch(1)
        val second = thread {
            secondStarted.countDown()
            secondToken = preflight.reserve(busy::get)
            if (secondToken != null) secondClientQueries++
            secondFinished.countDown()
        }

        assertTrue(secondStarted.await(2, TimeUnit.SECONDS))
        assertFalse(secondFinished.await(100, TimeUnit.MILLISECONDS))
        releaseHandoff.countDown()
        first.join(2_000)
        second.join(2_000)

        assertTrue(confirmation is ResumeConfirmation.Ready)
        assertNull(secondToken)
        assertEquals(0, secondClientQueries)
    }

    @Test
    fun `immediate batch acquires busy before releasing reservation`() {
        val busy = AtomicBoolean(false)
        val preflight = ResumePreflight<String>()
        val token = requireNotNull(preflight.reserve(busy::get))

        val result = preflight.finish(
            token,
            listOf(file("new", ResumeState.NONE)),
            acquireBusy = { busy.compareAndSet(false, true) }
        )

        assertTrue(result is ResumePreflightResult.Ready)
        assertTrue(busy.get())
        assertNull(preflight.reserve(busy::get))
    }

    @Test
    fun `prompt publication failure clears internal and public prompt even if reporting fails`() {
        val preflight = ResumePreflight<String>()
        val token = requireNotNull(preflight.reserve())
        val prompt = (preflight.finish(
            token, listOf(file("resume", ResumeState.AVAILABLE))
        ) as ResumePreflightResult.Waiting).prompt
        var publicPrompt: ResumePrompt? = prompt

        val published = publishResumePromptSafely(
            token = token,
            prompt = prompt,
            cancelPending = preflight::cancelReservation,
            publishPrompt = { error("notification failed") },
            clearPublicPrompt = { failedToken ->
                if (publicPrompt?.id == failedToken) publicPrompt = null
            },
            publishError = { _, _ -> error("notification still failed") }
        )

        assertFalse(published)
        assertNull(preflight.prompt)
        assertNull(publicPrompt)
    }

    @Test
    fun `failed prompt error publication preserves newer internal and public prompt`() {
        val preflight = ResumePreflight<String>()
        val tokenA = requireNotNull(preflight.reserve())
        val promptA = (preflight.finish(
            tokenA, listOf(file("a", ResumeState.AVAILABLE))
        ) as ResumePreflightResult.Waiting).prompt
        val publicState = java.util.concurrent.atomic.AtomicReference(
            ServiceTransferState(resumePrompt = promptA)
        )
        val aCleared = CountDownLatch(1)
        val bPublished = CountDownLatch(1)

        val failedPublisher = thread {
            publishResumePromptSafely(
                token = tokenA,
                prompt = promptA,
                cancelPending = preflight::cancelReservation,
                publishPrompt = { error("A notification failed") },
                clearPublicPrompt = { failedToken ->
                    publicState.updateAndGet { current ->
                        if (current.resumePrompt?.id == failedToken) {
                            current.copy(resumePrompt = null)
                        } else current
                    }
                    aCleared.countDown()
                    assertTrue(bPublished.await(2, TimeUnit.SECONDS))
                },
                publishError = { failedToken, error ->
                    publicState.updateAndGet { current ->
                        current.withResumePromptPublicationFailure(
                            failedToken,
                            "prompt failed: ${error.message}"
                        )
                    }
                }
            )
        }

        assertTrue(aCleared.await(2, TimeUnit.SECONDS))
        val tokenB = requireNotNull(preflight.reserve())
        val promptB = (preflight.finish(
            tokenB, listOf(file("b", ResumeState.AVAILABLE))
        ) as ResumePreflightResult.Waiting).prompt
        publicState.updateAndGet { it.copy(resumePrompt = promptB) }
        bPublished.countDown()
        failedPublisher.join(2_000)

        assertEquals(promptB, preflight.prompt)
        assertEquals(promptB, publicState.get().resumePrompt)
        assertEquals("prompt failed: A notification failed", publicState.get().serviceMessage)
    }

    private fun waitingBatch(): ResumePreflight<String> = ResumePreflight<String>().also { preflight ->
        val token = requireNotNull(preflight.reserve())
        preflight.finish(token, listOf(
            file("resume", ResumeState.AVAILABLE),
            file("new", ResumeState.NONE),
            file("invalid", ResumeState.INVALID)
        ))
    }

    private fun file(name: String, state: ResumeState) = ResumePreflightFile(
        value = name,
        fileName = name,
        status = ResumeStatus(state)
    )

    private fun runIncoming(acquire: () -> Unit): Boolean {
        acquire()
        return true
    }
}
