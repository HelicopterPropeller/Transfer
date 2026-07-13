package com.example.transfer.transfer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.util.Collections
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

class TransferPauseControllerTest {
    @Test
    fun `checkpoint pauses then resumes exactly once`() {
        val controller = TransferPauseController()
        val states = Collections.synchronizedList(mutableListOf<TransferPauseState>())

        assertTrue(controller.requestPause())
        val worker = thread {
            controller.checkpoint(
                sendPause = { states += TransferPauseState.PAUSING },
                sendResume = { states += TransferPauseState.RUNNING },
                onState = { states += it }
            )
        }

        eventually { controller.state == TransferPauseState.PAUSED }
        assertTrue(controller.requestResume())
        worker.join(2_000)

        assertEquals(TransferPauseState.RUNNING, controller.state)
        assertFalse(worker.isAlive)
        assertEquals(
            listOf(
                TransferPauseState.PAUSING,
                TransferPauseState.PAUSED,
                TransferPauseState.RUNNING,
                TransferPauseState.RUNNING
            ),
            states
        )
    }

    @Test
    fun `controller supports repeated pause resume cycles`() {
        val controller = TransferPauseController()

        repeat(2) {
            assertTrue(controller.requestPause())
            val worker = thread { controller.checkpoint({}, {}, {}) }
            eventually { controller.state == TransferPauseState.PAUSED }
            assertTrue(controller.requestResume())
            worker.join(2_000)
            assertFalse(worker.isAlive)
            assertEquals(TransferPauseState.RUNNING, controller.state)
        }
    }

    @Test
    fun `cancel wakes a paused checkpoint`() {
        val controller = TransferPauseController()
        val workerFailure = AtomicReference<Throwable?>()
        controller.requestPause()
        val worker = thread {
            try {
                org.junit.Assert.assertThrows(CancellationException::class.java) {
                    controller.checkpoint({}, {}, {})
                }
            } catch (failure: Throwable) {
                workerFailure.set(failure)
            }
        }

        eventually { controller.state == TransferPauseState.PAUSED }
        controller.cancel()
        worker.join(2_000)

        workerFailure.get()?.let { throw AssertionError("Worker assertion failed", it) }
        assertFalse(worker.isAlive)
        assertEquals(TransferPauseState.CANCELLED, controller.state)
    }

    @Test
    fun `await between files pauses and resumes without wire callbacks`() {
        val controller = TransferPauseController()
        val states = Collections.synchronizedList(mutableListOf<TransferPauseState>())

        assertTrue(controller.requestPause())
        val worker = thread { controller.awaitBetweenFiles { states += it } }
        eventually { controller.state == TransferPauseState.PAUSED }
        assertTrue(controller.requestResume())
        worker.join(2_000)

        assertFalse(worker.isAlive)
        assertEquals(TransferPauseState.RUNNING, controller.state)
        assertEquals(
            listOf(TransferPauseState.PAUSED, TransferPauseState.RUNNING),
            states
        )
    }

    private fun eventually(condition: () -> Boolean) {
        val deadline = System.nanoTime() + 2_000_000_000L
        while (System.nanoTime() < deadline) {
            if (condition()) return
            Thread.sleep(10)
        }
        fail("Condition was not met within 2 seconds")
    }
}
