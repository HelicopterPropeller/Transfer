package com.example.transfer.service

import com.example.transfer.ui.SelectedFile
import com.example.transfer.transfer.TransferPauseState
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test

class ServiceTransferStateTest {
    @Test
    fun `outgoing cancellation and failure preserve exact incoming state`() {
        val incoming = ServiceTransfer(
            direction = "接收",
            fileName = "incoming.bin",
            progress = 63,
            message = "正在接收",
            active = true,
            incomingAttemptId = 12L
        )
        val state = ServiceTransferState(
            outgoingTransfer = ServiceTransfer(
                direction = "发送",
                fileName = "outgoing.bin",
                progress = 35,
                message = "正在发送",
                active = true
            ),
            incomingTransfer = incoming
        )

        val cancelled = state.withCancelledOutgoing("传输已取消")
        val failed = state.withInactiveBatchFailure("批次失败")

        assertEquals(incoming, cancelled.incomingTransfer)
        assertEquals(incoming, failed.incomingTransfer)
    }

    @Test
    fun `cancelled outgoing transfer becomes inactive without losing batch position`() {
        val state = ServiceTransferState(
            outgoingTransfer = ServiceTransfer(
                direction = "发送",
                fileName = "b.bin",
                progress = 35,
                message = "正在发送",
                active = true,
                fileIndex = 2,
                fileCount = 4,
                batchProgress = 48,
            )
        )

        val cancelled = state.withCancelledOutgoing("传输已取消").outgoingTransfer!!

        assertFalse(cancelled.active)
        assertEquals("传输已取消", cancelled.message)
        assertEquals(TransferPauseState.CANCELLED, cancelled.pauseState)
        assertEquals(2, cancelled.fileIndex)
        assertEquals(4, cancelled.fileCount)
        assertEquals(48, cancelled.batchProgress)
    }

    @Test
    fun `recoverable batch exposes only unfinished selected files`() {
        val files = listOf(
            SelectedFile("content://a", "a.bin", "application/octet-stream", 4L)
        )

        val recoverable = RecoverableOutgoingBatch("batch-1", "peer-1", files)

        assertEquals("batch-1", recoverable.batchId)
        assertEquals("peer-1", recoverable.peerDeviceId)
        assertEquals(files, recoverable.files)
    }
}
