package com.example.transfer.transfer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.IOException
import java.util.concurrent.CancellationException

class TransferBatchRunnerTest {
    @Test
    fun `resumed item emits initial file and batch progress before sender chunks`() = runBlocking {
        val events = mutableListOf<String>()
        val runner = TransferBatchRunner(TransferPauseController()) { file, progress ->
            events += "send:${file.displayName}"
            progress(FileTransferProgress(80, file.length))
            Result.success(Unit)
        }
        val snapshots = mutableListOf<BatchTransferProgress>()

        runner.runItems(
            listOf(BatchTransferItem(source("resume", 100), initialConfirmedBytes = 40)),
            onProgress = { snapshots += it; events += "progress:${it.fileProgress}" },
            onPauseState = {}
        )

        assertEquals("progress:40", events.first())
        assertEquals(40, snapshots.first().fileProgress)
        assertEquals(40, snapshots.first().batchProgress)
    }

    @Test
    fun `failed resumed file contributes latest confirmed bytes once to following file`() = runBlocking {
        val runner = TransferBatchRunner(TransferPauseController()) { file, progress ->
            when (file.displayName) {
                "a" -> {
                    progress(FileTransferProgress(50, file.length))
                    Result.failure(IOException("lost"))
                }
                else -> Result.success(Unit)
            }
        }
        val snapshots = mutableListOf<BatchTransferProgress>()

        runner.runItems(
            listOf(
                BatchTransferItem(source("a", 100), initialConfirmedBytes = 40),
                BatchTransferItem(source("b", 100), initialConfirmedBytes = 20)
            ),
            snapshots::add,
            {}
        )

        val initialB = snapshots.first { it.fileName == "b" }
        assertEquals(35, initialB.batchProgress)
        assertEquals(20, initialB.fileProgress)
    }

    @Test
    fun `failure summary includes two details and remaining count`() {
        val result = BatchTransferResult(
            successCount = 1,
            failures = listOf(
                BatchFailure("a.txt", "broken"),
                BatchFailure("b.txt", "denied"),
                BatchFailure("c.txt", "offline")
            )
        )

        assertEquals(
            "发送完成：成功 1，失败 3；a.txt: broken；b.txt: denied；另有 1 项",
            formatBatchCompletion(result)
        )
    }

    @Test
    fun `success summary does not add failure detail separator`() {
        assertEquals(
            "发送完成：成功 2，失败 0",
            formatBatchCompletion(BatchTransferResult(2, emptyList()))
        )
    }

    @Test
    fun `resume control failure does not poison later files`() = runBlocking {
        val controller = TransferPauseController()
        val starts = mutableListOf<Pair<String, TransferPauseState>>()
        val runner = TransferBatchRunner(controller) { file, _ ->
            starts += file.displayName to controller.state
            assertTrue(controller.requestPause())
            assertTrue(controller.requestResume())
            if (file.displayName == "a") {
                controller.checkpoint(
                    sendPause = {},
                    sendResume = { throw IOException("resume control failed") },
                    onState = {}
                )
            } else {
                controller.checkpoint({}, {}, {})
            }
            Result.success(Unit)
        }

        val batch = async(Dispatchers.Default) {
            runner.run(listOf(source("a", 1), source("b", 1)), {}, {})
        }
        try {
            val result = withTimeout(2_000) { batch.await() }

            assertEquals(
                listOf(
                    "a" to TransferPauseState.RUNNING,
                    "b" to TransferPauseState.RUNNING
                ),
                starts
            )
            assertEquals(listOf(BatchFailure("a", "resume control failed")), result.failures)
            assertEquals(1, result.successCount)
            assertEquals(TransferPauseState.RUNNING, controller.state)
        } finally {
            if (!batch.isCompleted) controller.cancel()
            withTimeout(2_000) { batch.cancelAndJoin() }
        }
    }

    @Test
    fun `files run in order and failure does not stop queue`() = runBlocking {
        val calls = mutableListOf<String>()
        val files = listOf(source("a", 10), source("b", 20), source("c", 30))
        val runner = TransferBatchRunner(TransferPauseController()) { file, progress ->
            calls += file.displayName
            progress(FileTransferProgress(file.length, file.length))
            if (file.displayName == "b") {
                Result.failure(IOException("broken"))
            } else {
                Result.success(Unit)
            }
        }

        val snapshots = mutableListOf<BatchTransferProgress>()
        val result = runner.run(files, snapshots::add) {}

        assertEquals(listOf("a", "b", "c"), calls)
        assertEquals(2, result.successCount)
        assertEquals(listOf(BatchFailure("b", "broken")), result.failures)
        assertEquals(100, snapshots.last().batchProgress)
    }

    @Test
    fun `batch progress accumulates only receiver confirmed bytes`() = runBlocking {
        val files = listOf(source("a", 10), source("b", 30))
        val runner = TransferBatchRunner(TransferPauseController()) { file, progress ->
            when (file.displayName) {
                "a" -> progress(FileTransferProgress(5, 10))
                "b" -> progress(FileTransferProgress(15, 30))
            }
            Result.success(Unit)
        }

        val snapshots = mutableListOf<BatchTransferProgress>()
        runner.run(files, snapshots::add) {}

        assertEquals(12, snapshots[0].batchProgress)
        assertEquals(50, snapshots[0].fileProgress)
        assertEquals(62, snapshots[1].batchProgress)
        assertEquals(50, snapshots[1].fileProgress)
    }

    @Test
    fun `failed file carries only its confirmed bytes into next file progress`() = runBlocking {
        val files = listOf(source("a", 10), source("b", 20), source("c", 30))
        val runner = TransferBatchRunner(TransferPauseController()) { file, progress ->
            when (file.displayName) {
                "a" -> {
                    progress(FileTransferProgress(10, 10))
                    Result.success(Unit)
                }
                "b" -> {
                    progress(FileTransferProgress(5, 20))
                    Result.failure(IOException("partial"))
                }
                else -> {
                    progress(FileTransferProgress(0, 30))
                    Result.success(Unit)
                }
            }
        }
        val snapshots = mutableListOf<BatchTransferProgress>()

        runner.run(files, snapshots::add) {}

        val firstCProgress = snapshots.first { it.fileName == "c" }
        assertEquals(25, firstCProgress.batchProgress)
    }

    @Test
    fun `exception thrown by one sender is recorded and later files continue`() = runBlocking {
        val calls = mutableListOf<String>()
        val runner = TransferBatchRunner(TransferPauseController()) { file, _ ->
            calls += file.displayName
            when (file.displayName) {
                "b" -> throw IOException("thrown")
                else -> Result.success(Unit)
            }
        }

        val result = runner.run(
            listOf(source("a", 1), source("b", 1), source("c", 1)),
            onProgress = {},
            onPauseState = {}
        )

        assertEquals(listOf("a", "b", "c"), calls)
        assertEquals(2, result.successCount)
        assertEquals(listOf(BatchFailure("b", "thrown")), result.failures)
    }

    @Test
    fun `cancel after current file prevents next file from starting`() = runBlocking {
        val controller = TransferPauseController()
        val calls = mutableListOf<String>()
        val runner = TransferBatchRunner(controller) { file, _ ->
            calls += file.displayName
            if (file.displayName == "a") {
                controller.cancel()
                Result.failure(IOException("transport cancelled"))
            } else {
                Result.success(Unit)
            }
        }

        try {
            runner.run(
                listOf(source("a", 1), source("b", 1)),
                onProgress = {},
                onPauseState = {}
            )
            org.junit.Assert.fail("Expected cancellation")
        } catch (_: CancellationException) {
            assertEquals(listOf("a"), calls)
        }
    }

    @Test
    fun `cancellation returned as failure is rethrown for a single file`() = runBlocking {
        val cancellation = CancellationException("cancelled result")
        val runner = TransferBatchRunner(TransferPauseController()) { _, _ ->
            Result.failure(cancellation)
        }

        try {
            runner.run(listOf(source("only", 1)), {}, {})
            org.junit.Assert.fail("Expected cancellation")
        } catch (actual: CancellationException) {
            assertSame(cancellation, actual)
        }
    }

    @Test
    fun `controller cancelled by final sender is checked after it returns`() = runBlocking {
        val controller = TransferPauseController()
        val runner = TransferBatchRunner(controller) { _, _ ->
            controller.cancel()
            Result.failure(IOException("socket closed"))
        }

        try {
            runner.run(listOf(source("only", 1)), {}, {})
            org.junit.Assert.fail("Expected cancellation")
        } catch (_: CancellationException) {
            assertEquals(TransferPauseState.CANCELLED, controller.state)
        }
    }

    @Test
    fun `negative source length is rejected before sending`() = runBlocking {
        var senderCalled = false
        val runner = TransferBatchRunner(TransferPauseController()) { _, _ ->
            senderCalled = true
            Result.success(Unit)
        }

        org.junit.Assert.assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                runner.run(listOf(source("invalid", -1)), {}, {})
            }
        }
        assertFalse(senderCalled)
    }

    @Test
    fun `total length overflow is rejected before sending`() = runBlocking {
        var senderCalled = false
        val runner = TransferBatchRunner(TransferPauseController()) { _, _ ->
            senderCalled = true
            Result.success(Unit)
        }

        org.junit.Assert.assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                runner.run(
                    listOf(source("huge", Long.MAX_VALUE), source("extra", 1)),
                    {},
                    {}
                )
            }
        }
        assertFalse(senderCalled)
    }

    @Test
    fun `failed final file still completes attempted batch progress`() = runBlocking {
        val runner = TransferBatchRunner(TransferPauseController()) { file, progress ->
            progress(FileTransferProgress(4, file.length))
            Result.failure(IOException("stopped"))
        }
        val snapshots = mutableListOf<BatchTransferProgress>()

        val result = runner.run(listOf(source("failed", 10)), snapshots::add) {}

        assertEquals(listOf(BatchFailure("failed", "stopped")), result.failures)
        assertEquals(40, snapshots.first().batchProgress)
        assertEquals(100, snapshots.last().batchProgress)
    }

    @Test
    fun `all zero byte queue finishes at one hundred percent`() = runBlocking {
        val calls = mutableListOf<String>()
        val runner = TransferBatchRunner(TransferPauseController()) { file, _ ->
            calls += file.displayName
            Result.success(Unit)
        }
        val snapshots = mutableListOf<BatchTransferProgress>()

        val result = runner.run(
            listOf(source("empty-a", 0), source("empty-b", 0)),
            snapshots::add,
            {}
        )

        assertEquals(listOf("empty-a", "empty-b"), calls)
        assertEquals(2, result.successCount)
        assertEquals(100, snapshots.last().batchProgress)
    }

    @Test
    fun `pause between files prevents next file until resume`() = runBlocking {
        val controller = TransferPauseController()
        val paused = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val calls = mutableListOf<String>()
        val runner = TransferBatchRunner(controller) { file, progress ->
            calls += file.displayName
            progress(FileTransferProgress(file.length, file.length))
            if (file.displayName == "a") assertTrue(controller.requestPause())
            if (file.displayName == "b") secondStarted.complete(Unit)
            Result.success(Unit)
        }

        val batch = async(Dispatchers.Default) {
            runner.run(
                listOf(source("a", 10), source("b", 20)),
                onProgress = {},
                onPauseState = { state ->
                    if (state == TransferPauseState.PAUSED) paused.complete(Unit)
                }
            )
        }

        try {
            withTimeout(2_000) { paused.await() }
            assertEquals(listOf("a"), calls)
            assertFalse(secondStarted.isCompleted)
            assertTrue(controller.requestResume())
            withTimeout(2_000) { secondStarted.await() }
            withTimeout(2_000) { batch.await() }
            assertEquals(listOf("a", "b"), calls)
        } finally {
            controller.cancel()
            withTimeout(2_000) { batch.cancelAndJoin() }
        }
    }

    private fun source(name: String, length: Long) = SendFileSource(
        displayName = name,
        mimeType = "application/octet-stream",
        length = length,
        sourceUri = "content://$name",
        openStream = { ByteArrayInputStream(ByteArray(0)) }
    )
}
