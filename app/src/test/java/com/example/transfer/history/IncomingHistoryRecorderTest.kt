package com.example.transfer.history

import java.io.IOException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class IncomingHistoryRecorderTest {
    @Test
    fun `successful incoming file records resolved peer and received uri`() = runBlocking {
        val store = IncomingRecordingHistoryStore()
        val recorder = IncomingHistoryRecorder(store) { address ->
            HistoryPeer("peer-a", "Pixel", address)
        }

        val historyId = recorder.start(
            fileName = "a.bin",
            fileSize = 4,
            mimeType = "application/octet-stream",
            peerAddress = "192.168.1.20"
        )
        recorder.succeed(historyId, "content://received/a.bin")

        assertEquals(
            TransferHistoryDraft(
                direction = TransferDirection.RECEIVE,
                fileName = "a.bin",
                fileSize = 4,
                mimeType = "application/octet-stream",
                peerId = "peer-a",
                peerName = "Pixel",
                peerAddress = "192.168.1.20"
            ),
            store.drafts.single()
        )
        assertEquals(
            listOf(TransferHistoryStatus.IN_PROGRESS, TransferHistoryStatus.SUCCESS),
            store.statuses
        )
        assertEquals("content://received/a.bin", store.receivedUris.single())
    }

    @Test
    fun `failed incoming file records original failure message`() = runBlocking {
        val store = IncomingRecordingHistoryStore()
        val recorder = IncomingHistoryRecorder(store)
        val historyId = recorder.start(
            fileName = "a.bin",
            fileSize = 4,
            mimeType = "application/octet-stream",
            peerAddress = "192.168.1.20"
        )

        recorder.fail(historyId, "Socket closed")

        assertEquals(
            listOf(TransferHistoryStatus.IN_PROGRESS, TransferHistoryStatus.FAILED),
            store.statuses
        )
        assertEquals(listOf("Socket closed"), store.errorMessages)
        assertEquals(listOf<String?>(null), store.receivedUris)
    }

    @Test
    fun `cancelled incoming file records cancellation reason`() = runBlocking {
        val store = IncomingRecordingHistoryStore()
        val recorder = IncomingHistoryRecorder(store)
        val historyId = recorder.start(
            fileName = "a.bin",
            fileSize = 4,
            mimeType = "application/octet-stream",
            peerAddress = "192.168.1.20"
        )

        recorder.cancel(historyId, "Receive service stopped")

        assertEquals(
            listOf(TransferHistoryStatus.IN_PROGRESS, TransferHistoryStatus.CANCELLED),
            store.statuses
        )
        assertEquals(listOf("Receive service stopped"), store.errorMessages)
        assertEquals(listOf<String?>(null), store.receivedUris)
    }

    @Test
    fun `ordinary history failures are best effort`() = runBlocking {
        val recorder = IncomingHistoryRecorder(IncomingThrowingHistoryStore())

        val historyId = recorder.start(
            fileName = "a.bin",
            fileSize = 4,
            mimeType = "application/octet-stream",
            peerAddress = "192.168.1.20"
        )

        assertNull(historyId)
        recorder.succeed(1, "content://received/a.bin")
        recorder.fail(1, "Socket closed")
        recorder.cancel(1, "Receive service stopped")
    }
}

private class IncomingRecordingHistoryStore : TransferHistoryStore {
    val drafts = mutableListOf<TransferHistoryDraft>()
    val statuses = mutableListOf<TransferHistoryStatus>()
    val errorMessages = mutableListOf<String?>()
    val receivedUris = mutableListOf<String?>()

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
        statuses += status
        errorMessages += errorMessage
        receivedUris += receivedUri
        return true
    }

    override suspend fun interruptActive(): Int = 0
    override suspend fun delete(id: Long): Boolean = false
    override suspend fun clear(): Boolean = false
}

private class IncomingThrowingHistoryStore : TransferHistoryStore {
    override fun observeAll(): Flow<List<TransferHistoryEntry>> = emptyFlow()
    override suspend fun start(draft: TransferHistoryDraft): Long? = throw unavailable()

    override suspend fun finish(
        id: Long,
        status: TransferHistoryStatus,
        errorMessage: String?,
        receivedUri: String?
    ): Boolean = throw unavailable()

    override suspend fun interruptActive(): Int = throw unavailable()
    override suspend fun delete(id: Long): Boolean = throw unavailable()
    override suspend fun clear(): Boolean = throw unavailable()

    private fun unavailable() = IOException("database unavailable")
}
