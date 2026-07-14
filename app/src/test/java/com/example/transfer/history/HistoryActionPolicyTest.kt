package com.example.transfer.history

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class HistoryActionPolicyTest {
    @Test
    fun `only successful received files with a uri can be opened`() {
        val opener = FakeHistoryFileOpener()

        assertEquals(
            HistoryOpenError.FILE_UNAVAILABLE,
            HistoryFileActions.open(entry(direction = TransferDirection.SEND), opener)
        )
        assertEquals(
            HistoryOpenError.FILE_UNAVAILABLE,
            HistoryFileActions.open(entry(status = TransferHistoryStatus.FAILED), opener)
        )
        assertEquals(
            HistoryOpenError.FILE_UNAVAILABLE,
            HistoryFileActions.open(entry(receivedUri = null), opener)
        )
    }

    @Test
    fun `open validates readability and applies mime fallback`() {
        val opener = FakeHistoryFileOpener()

        val error = HistoryFileActions.open(entry(mimeType = ""), opener)

        assertNull(error)
        assertEquals("content://received/a.bin", opener.openedUri)
        assertEquals("application/octet-stream", opener.openedMimeType)
    }

    @Test
    fun `unreadable received uri returns file unavailable`() {
        val opener = FakeHistoryFileOpener(readFailure = SecurityException("denied"))

        assertEquals(
            HistoryOpenError.FILE_UNAVAILABLE,
            HistoryFileActions.open(entry(), opener)
        )
    }

    @Test
    fun `missing view handler returns no application`() {
        val opener = FakeHistoryFileOpener(hasHandler = false)

        assertEquals(
            HistoryOpenError.NO_HANDLER,
            HistoryFileActions.open(entry(), opener)
        )
    }

    private fun entry(
        direction: TransferDirection = TransferDirection.RECEIVE,
        status: TransferHistoryStatus = TransferHistoryStatus.SUCCESS,
        mimeType: String = "application/octet-stream",
        receivedUri: String? = "content://received/a.bin"
    ) = TransferHistoryEntry(
        id = 1,
        direction = direction,
        fileName = "a.bin",
        fileSize = 4,
        mimeType = mimeType,
        peerId = null,
        peerName = null,
        peerAddress = null,
        status = status,
        startedAt = 1,
        finishedAt = 2,
        errorMessage = null,
        sourceUri = null,
        receivedUri = receivedUri
    )
}

private class FakeHistoryFileOpener(
    private val hasHandler: Boolean = true,
    private val readFailure: Exception? = null
) : HistoryFileOpener {
    var openedUri: String? = null
    var openedMimeType: String? = null

    override fun ensureReadable(uri: String) {
        readFailure?.let { throw it }
    }

    override fun hasViewHandler(uri: String, mimeType: String): Boolean = hasHandler

    override fun open(uri: String, mimeType: String) {
        openedUri = uri
        openedMimeType = mimeType
    }
}
