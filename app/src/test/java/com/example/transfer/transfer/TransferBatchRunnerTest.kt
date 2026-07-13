package com.example.transfer.transfer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.IOException

class TransferBatchRunnerTest {
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

        withTimeout(2_000) { paused.await() }
        assertEquals(listOf("a"), calls)
        assertFalse(secondStarted.isCompleted)
        assertTrue(controller.requestResume())
        withTimeout(2_000) { secondStarted.await() }
        withTimeout(2_000) { batch.await() }
        assertEquals(listOf("a", "b"), calls)
    }

    private fun source(name: String, length: Long) = SendFileSource(
        displayName = name,
        mimeType = "application/octet-stream",
        length = length,
        openStream = { ByteArrayInputStream(ByteArray(0)) }
    )
}
