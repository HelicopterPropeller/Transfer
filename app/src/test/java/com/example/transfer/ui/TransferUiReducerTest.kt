package com.example.transfer.ui

import com.example.transfer.discovery.DiscoveredDevice
import com.example.transfer.service.ServiceTransfer
import com.example.transfer.service.ServiceTransferState
import com.example.transfer.transfer.TransferPauseState
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.InetAddress

class TransferUiReducerTest {
    private val peer = DiscoveredDevice(
        "peer", "Pixel", InetAddress.getLoopbackAddress(), 42043, 1
    )

    @Test
    fun `send requires a non-empty valid file list`() {
        var state = TransferUiState()
        assertFalse(state.canSend)

        state = TransferUiReducer.withDevices(state, listOf(peer))
        state = TransferUiReducer.selectDevice(state, "peer")
        assertFalse(state.canSend)

        state = state.copy(selectedFiles = listOf(
            SelectedFile("content://a", "a.txt", "text/plain", 4),
            SelectedFile("content://b", "b.txt", "text/plain", 8)
        ))
        assertTrue(state.canSend)

        state = state.copy(selectedFiles = listOf(
            SelectedFile("content://bad", "bad.txt", "text/plain", -1)
        ))
        assertFalse(state.canSend)
    }

    @Test
    fun `running outgoing transfer can pause but cannot resume`() {
        val status = TransferStatus(
            "发送", "a.txt", 40, "正在发送", true,
            fileIndex = 1, fileCount = 2, batchProgress = 20,
            pauseState = TransferPauseState.RUNNING
        )

        val state = TransferUiState(transfer = status)

        assertTrue(state.canPause)
        assertFalse(state.canResume)
    }

    @Test
    fun `paused outgoing transfer can resume but cannot pause`() {
        val status = TransferStatus(
            "发送", "a.txt", 40, "已暂停", true,
            fileIndex = 1, fileCount = 2, batchProgress = 20,
            pauseState = TransferPauseState.PAUSED
        )

        val state = TransferUiState(transfer = status)

        assertFalse(state.canPause)
        assertTrue(state.canResume)
    }

    @Test
    fun `incoming and inactive transfers expose no pause control`() {
        val incoming = TransferUiState(transfer = TransferStatus(
            "接收", "a.txt", 40, "正在接收", true,
            pauseState = TransferPauseState.RUNNING
        ))
        val inactive = TransferUiState(transfer = TransferStatus(
            "发送", "a.txt", 100, "发送完成", false,
            pauseState = TransferPauseState.PAUSED
        ))

        assertFalse(incoming.canPause)
        assertFalse(incoming.canResume)
        assertFalse(inactive.canPause)
        assertFalse(inactive.canResume)
    }

    @Test
    fun `expired selected peer is cleared`() {
        var state = TransferUiState(devices = listOf(peer), selectedDeviceId = "peer")
        state = TransferUiReducer.withDevices(state, emptyList())
        assertNull(state.selectedDeviceId)
    }

    @Test
    fun `service state maps batch progress and preserves selected files`() {
        val files = listOf(
            SelectedFile("content://a", "a.txt", "text/plain", 4),
            SelectedFile("content://b", "b.txt", "text/plain", 8)
        )
        val current = TransferUiState(selectedFiles = files)
        val service = ServiceTransferState(
            devices = listOf(peer),
            serviceMessage = "后台运行中",
            transfer = ServiceTransfer(
                "发送", "movie.mp4", 55, "正在发送", true,
                fileIndex = 2, fileCount = 3, batchProgress = 60,
                pauseState = TransferPauseState.PAUSING
            )
        )
        val result = TransferUiReducer.withServiceState(current, service)
        assertEquals(files, result.selectedFiles)
        assertEquals(55, result.transfer?.progress)
        assertEquals(2, result.transfer?.fileIndex)
        assertEquals(3, result.transfer?.fileCount)
        assertEquals(60, result.transfer?.batchProgress)
        assertEquals(TransferPauseState.PAUSING, result.transfer?.pauseState)
        assertEquals("后台运行中", result.serviceStatus)
    }
}
