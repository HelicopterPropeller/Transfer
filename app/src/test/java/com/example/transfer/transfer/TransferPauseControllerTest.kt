package com.example.transfer.transfer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.util.Collections
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

class TransferPauseControllerTest {
    @Test
    fun `checkpoint pauses then resumes exactly once`() {
        val controller = TransferPauseController()
        val states = Collections.synchronizedList(mutableListOf<TransferPauseState>())

        assertTrue(controller.requestPause())
        val worker = startWorker {
            controller.checkpoint(
                sendPause = { states += TransferPauseState.PAUSING },
                sendResume = { states += TransferPauseState.RUNNING },
                onState = { states += it }
            )
        }

        eventually { controller.state == TransferPauseState.PAUSED }
        assertTrue(controller.requestResume())
        worker.joinAndRethrow()

        assertEquals(TransferPauseState.RUNNING, controller.state)
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
            val worker = startWorker { controller.checkpoint({}, {}, {}) }
            eventually { controller.state == TransferPauseState.PAUSED }
            assertTrue(controller.requestResume())
            worker.joinAndRethrow()
            assertEquals(TransferPauseState.RUNNING, controller.state)
        }
    }

    @Test
    fun `cancel wakes a paused checkpoint`() {
        val controller = TransferPauseController()
        controller.requestPause()
        val worker = startWorker {
            org.junit.Assert.assertThrows(CancellationException::class.java) {
                controller.checkpoint({}, {}, {})
            }
        }

        eventually { controller.state == TransferPauseState.PAUSED }
        controller.cancel()
        worker.joinAndRethrow()

        assertEquals(TransferPauseState.CANCELLED, controller.state)
    }

    @Test
    fun `cancel during resume remains terminal and suppresses running state`() {
        val controller = TransferPauseController()
        val resumeStarted = CountDownLatch(1)
        val finishResume = CountDownLatch(1)
        val states = Collections.synchronizedList(mutableListOf<TransferPauseState>())

        assertTrue(controller.requestPause())
        val worker = startWorker {
            org.junit.Assert.assertThrows(CancellationException::class.java) {
                controller.checkpoint(
                    sendPause = {},
                    sendResume = {
                        resumeStarted.countDown()
                        assertTrue(finishResume.await(2, TimeUnit.SECONDS))
                    },
                    onState = { states += it }
                )
            }
        }

        eventually { controller.state == TransferPauseState.PAUSED }
        assertTrue(controller.requestResume())
        assertTrue(resumeStarted.await(2, TimeUnit.SECONDS))
        controller.cancel()
        finishResume.countDown()
        worker.joinAndRethrow()

        assertEquals(TransferPauseState.CANCELLED, controller.state)
        assertFalse(states.contains(TransferPauseState.RUNNING))
    }

    @Test
    fun `duplicate resume during resume callback does not skip next pause`() {
        val controller = TransferPauseController()
        val resumeStarted = CountDownLatch(1)
        val finishResume = CountDownLatch(1)

        assertTrue(controller.requestPause())
        val firstWorker = startWorker {
            controller.checkpoint(
                sendPause = {},
                sendResume = {
                    resumeStarted.countDown()
                    assertTrue(finishResume.await(2, TimeUnit.SECONDS))
                },
                onState = {}
            )
        }

        eventually { controller.state == TransferPauseState.PAUSED }
        assertTrue(controller.requestResume())
        assertTrue(resumeStarted.await(2, TimeUnit.SECONDS))
        val duplicateResumeAccepted = controller.requestResume()
        finishResume.countDown()
        firstWorker.joinAndRethrow()

        assertTrue(controller.requestPause())
        val secondWorker = startWorker { controller.checkpoint({}, {}, {}) }
        eventually { controller.state == TransferPauseState.PAUSED }
        assertTrue(secondWorker.isAlive)
        assertTrue(controller.requestResume())
        secondWorker.joinAndRethrow()

        assertFalse(duplicateResumeAccepted)
        assertEquals(TransferPauseState.RUNNING, controller.state)
    }

    @Test
    fun `running callback can coordinate with cancel without holding controller lock`() {
        val controller = TransferPauseController()
        val runningCallbackEntered = CountDownLatch(1)
        val cancelCompleted = CountDownLatch(1)
        val cancelWorker = startWorker {
            assertTrue(runningCallbackEntered.await(2, TimeUnit.SECONDS))
            controller.cancel()
            cancelCompleted.countDown()
        }

        assertTrue(controller.requestPause())
        val checkpointWorker = startWorker {
            controller.checkpoint(
                sendPause = {},
                sendResume = {},
                onState = { state ->
                    if (state == TransferPauseState.RUNNING) {
                        runningCallbackEntered.countDown()
                        assertTrue(cancelCompleted.await(1, TimeUnit.SECONDS))
                    }
                }
            )
        }

        eventually { controller.state == TransferPauseState.PAUSED }
        assertTrue(controller.requestResume())
        cancelWorker.joinAndRethrow()
        checkpointWorker.joinAndRethrow()

        assertEquals(TransferPauseState.CANCELLED, controller.state)
    }

    @Test
    fun `await between files pauses and resumes without wire callbacks`() {
        val controller = TransferPauseController()
        val states = Collections.synchronizedList(mutableListOf<TransferPauseState>())

        assertTrue(controller.requestPause())
        val worker = startWorker { controller.awaitBetweenFiles { states += it } }
        eventually { controller.state == TransferPauseState.PAUSED }
        assertTrue(controller.requestResume())
        worker.joinAndRethrow()

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

    private fun startWorker(block: () -> Unit): Worker {
        val failure = AtomicReference<Throwable?>()
        val thread = thread(isDaemon = true) {
            try {
                block()
            } catch (throwable: Throwable) {
                failure.set(throwable)
            }
        }
        return Worker(thread, failure)
    }

    private class Worker(
        private val thread: Thread,
        private val failure: AtomicReference<Throwable?>
    ) {
        val isAlive: Boolean
            get() = thread.isAlive

        fun joinAndRethrow() {
            thread.join(2_000)
            assertFalse("Worker did not finish within 2 seconds", thread.isAlive)
            failure.get()?.let { throw AssertionError("Worker failed", it) }
        }
    }
}
