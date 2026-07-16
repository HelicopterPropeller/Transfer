package com.example.transfer.service

import com.example.transfer.ui.SelectedFile
import org.junit.Assert.assertEquals
import org.junit.Test

class ServiceTransferStateTest {
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
