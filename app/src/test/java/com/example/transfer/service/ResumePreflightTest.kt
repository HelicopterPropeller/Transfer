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
        val confirmation = preflight.confirm(prompt.id, ResumeChoice.RESUME_AVAILABLE)

        val started = confirmation.startIfReady { false }

        assertFalse(started)
        assertNull(preflight.prompt)
        assertFalse(preflight.hasPendingBatch)
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
