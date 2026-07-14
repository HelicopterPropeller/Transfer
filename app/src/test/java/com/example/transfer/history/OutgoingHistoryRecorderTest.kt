package com.example.transfer.history

import com.example.transfer.transfer.SendFileSource
import com.example.transfer.transfer.TransferPauseController
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.junit.Assert.assertEquals
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.IOException
import java.util.concurrent.CancellationException

class OutgoingHistoryRecorderTest {
    private val peer = HistoryPeer("peer-a", "Pixel", "192.168.1.20")

    @Test
    fun `successful outgoing file reaches success`() = runBlocking {
        val store = RecordingHistoryStore()
        val recorder = OutgoingHistoryRecorder(store)

        val result = recorder.send(source("content://a"), peer, TransferPauseController()) {
            Result.success(Unit)
        }

        assertTrue(result.isSuccess)
        assertEquals(
            TransferHistoryDraft(
                direction = TransferDirection.SEND,
                fileName = "a.bin",
                fileSize = 4,
                mimeType = "application/octet-stream",
                peerId = "peer-a",
                peerName = "Pixel",
                peerAddress = "192.168.1.20",
                sourceUri = "content://a"
            ),
            store.drafts.single()
        )
        assertEquals(
            listOf(TransferHistoryStatus.IN_PROGRESS, TransferHistoryStatus.SUCCESS),
            store.statuses
        )
    }

    @Test
    fun `failed outgoing file reaches failed and preserves result`() = runBlocking {
        val store = RecordingHistoryStore()
        val recorder = OutgoingHistoryRecorder(store)
        val failure = IOException("Socket closed")

        val result = recorder.send(source("content://a"), peer, TransferPauseController()) {
            Result.failure(failure)
        }

        assertSame(failure, result.exceptionOrNull())
        assertEquals(TransferHistoryStatus.FAILED, store.statuses.last())
        assertEquals(listOf("Socket closed"), store.errorMessages)
    }

    @Test
    fun `cancelled controller records cancelled without changing result`() = runBlocking {
        val store = RecordingHistoryStore()
        val controller = TransferPauseController().also { it.cancel() }
        val failure = IOException("Socket closed")

        val result = OutgoingHistoryRecorder(store).send(source("content://a"), peer, controller) {
            Result.failure(failure)
        }

        assertSame(failure, result.exceptionOrNull())
        assertEquals(TransferHistoryStatus.CANCELLED, store.statuses.last())
    }

    @Test
    fun `history insert failure still sends file`() = runBlocking {
        var calls = 0
        val recorder = OutgoingHistoryRecorder(FailingStartHistoryStore())

        val result = recorder.send(source("content://a"), peer, TransferPauseController()) {
            calls++
            Result.success(Unit)
        }

        assertTrue(result.isSuccess)
        assertEquals(1, calls)
    }

    @Test
    fun `thrown cancellation is recorded and rethrown`() = runBlocking {
        val store = RecordingHistoryStore()
        val cancellation = CancellationException("cancelled")

        try {
            OutgoingHistoryRecorder(store).send(source("content://a"), peer, TransferPauseController()) {
                throw cancellation
            }
            fail("Expected cancellation")
        } catch (actual: CancellationException) {
            assertSame(cancellation, actual)
        }

        assertEquals(TransferHistoryStatus.CANCELLED, store.statuses.last())
        assertEquals(listOf("cancelled"), store.errorMessages)
    }

    @Test
    fun `cancelled job still records cancelled terminal status`() = runBlocking {
        val store = RecordingHistoryStore(beforeFinish = { yield() })
        val recorder = OutgoingHistoryRecorder(store)
        val sendJob = launch(start = CoroutineStart.UNDISPATCHED) {
            recorder.send(source("content://a"), peer, TransferPauseController()) {
                awaitCancellation()
            }
        }

        sendJob.cancelAndJoin()

        assertEquals(
            listOf(TransferHistoryStatus.IN_PROGRESS, TransferHistoryStatus.CANCELLED),
            store.statuses
        )
    }

    @Test
    fun `finish failure does not replace original cancellation`() = runBlocking {
        val finishFailure = IOException("history unavailable")
        val store = RecordingHistoryStore(beforeFinish = { throw finishFailure })
        val cancellation = CancellationException("send cancelled")

        try {
            OutgoingHistoryRecorder(store).send(
                source("content://a"),
                peer,
                TransferPauseController()
            ) {
                throw cancellation
            }
            fail("Expected cancellation")
        } catch (actual: Throwable) {
            assertSame(cancellation, actual)
        }
    }

    @Test
    fun `other thrown exception is recorded and rethrown`() = runBlocking {
        val store = RecordingHistoryStore()
        val failure = IOException("broken")

        try {
            OutgoingHistoryRecorder(store).send(source("content://a"), peer, TransferPauseController()) {
                throw failure
            }
            fail("Expected failure")
        } catch (actual: IOException) {
            assertSame(failure, actual)
        }

        assertEquals(TransferHistoryStatus.FAILED, store.statuses.last())
        assertEquals(listOf("broken"), store.errorMessages)
    }

    private fun source(uri: String) = SendFileSource(
        displayName = "a.bin",
        mimeType = "application/octet-stream",
        length = 4,
        sourceUri = uri,
        openStream = { ByteArrayInputStream(byteArrayOf(1, 2, 3, 4)) }
    )
}

private class RecordingHistoryStore(
    private val beforeFinish: suspend () -> Unit = {}
) : TransferHistoryStore {
    val drafts = mutableListOf<TransferHistoryDraft>()
    val statuses = mutableListOf<TransferHistoryStatus>()
    val errorMessages = mutableListOf<String?>()

    override fun observeAll(): Flow<List<TransferHistoryEntry>> = emptyFlow()

    override suspend fun start(draft: TransferHistoryDraft): Long {
        drafts += draft
        statuses += TransferHistoryStatus.IN_PROGRESS
        return drafts.size.toLong()
    }

    override suspend fun finish(
        id: Long,
        status: TransferHistoryStatus,
        errorMessage: String?,
        receivedUri: String?
    ): Boolean {
        beforeFinish()
        statuses += status
        errorMessages += errorMessage
        return true
    }

    override suspend fun interruptActive(): Int = 0

    override suspend fun delete(id: Long): Boolean = false

    override suspend fun clear(): Boolean = false
}

private class FailingStartHistoryStore : TransferHistoryStore {
    override fun observeAll(): Flow<List<TransferHistoryEntry>> = emptyFlow()

    override suspend fun start(draft: TransferHistoryDraft): Long? = null

    override suspend fun finish(
        id: Long,
        status: TransferHistoryStatus,
        errorMessage: String?,
        receivedUri: String?
    ): Boolean = false

    override suspend fun interruptActive(): Int = 0

    override suspend fun delete(id: Long): Boolean = false

    override suspend fun clear(): Boolean = false
}
