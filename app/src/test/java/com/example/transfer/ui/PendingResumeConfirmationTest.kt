package com.example.transfer.ui

import com.example.transfer.service.ResumeChoice
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class PendingResumeConfirmationTest {
    @Test
    fun `disconnect click reconnect retries confirmation exactly once`() {
        val controller = PendingResumeConfirmationController()
        val calls = mutableListOf<PendingResumeConfirmation>()

        controller.confirm(4, ResumeChoice.RESUME_AVAILABLE)
        controller.attach { calls += it }
        controller.attach { calls += it }

        assertEquals(
            listOf(PendingResumeConfirmation(4, ResumeChoice.RESUME_AVAILABLE)),
            calls
        )
        assertNull(controller.pending)
    }

    @Test
    fun `cancel choice is also queued while disconnected`() {
        val controller = PendingResumeConfirmationController()
        val calls = mutableListOf<PendingResumeConfirmation>()

        controller.confirm(8, ResumeChoice.CANCEL)
        controller.attach { calls += it }

        assertEquals(listOf(PendingResumeConfirmation(8, ResumeChoice.CANCEL)), calls)
    }

    @Test
    fun `new prompt replaces obsolete disconnected confirmation`() {
        val controller = PendingResumeConfirmationController()
        val calls = mutableListOf<PendingResumeConfirmation>()

        controller.confirm(1, ResumeChoice.RESTART_ALL)
        controller.onPromptChanged(2)
        controller.confirm(2, ResumeChoice.RESUME_AVAILABLE)
        controller.attach { calls += it }

        assertEquals(listOf(PendingResumeConfirmation(2, ResumeChoice.RESUME_AVAILABLE)), calls)
    }

    @Test
    fun `confirmation attached to a service is sent once without queuing`() {
        val calls = mutableListOf<PendingResumeConfirmation>()
        val controller = PendingResumeConfirmationController()
        controller.attach { calls += it }

        controller.confirm(3, ResumeChoice.RESTART_ALL)
        controller.attach { calls += it }

        assertEquals(listOf(PendingResumeConfirmation(3, ResumeChoice.RESTART_ALL)), calls)
        assertNull(controller.pending)
    }

    @Test
    fun `duplicate callback for the same prompt is sent only once`() {
        val calls = mutableListOf<PendingResumeConfirmation>()
        val controller = PendingResumeConfirmationController()
        controller.attach { calls += it }

        controller.confirm(6, ResumeChoice.CANCEL)
        controller.confirm(6, ResumeChoice.CANCEL)

        assertEquals(listOf(PendingResumeConfirmation(6, ResumeChoice.CANCEL)), calls)
    }
}
