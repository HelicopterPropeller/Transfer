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
    private val peerA = DiscoveredDevice(
        "peer-a", "Pixel A", InetAddress.getByName("192.168.1.20"), 42043, 1
    )
    private val peerB = DiscoveredDevice(
        "peer-b", "Pixel B", InetAddress.getByName("192.168.1.21"), 42043, 1
    )
    private val selectedFile = SelectedFile("content://a", "a.bin", "application/octet-stream", 4)

    @Test
    fun `history retry selects original peer when it later appears`() {
        val restored = TransferUiReducer.restoreHistoryFile(
            TransferUiState(), selectedFile, preferredPeerId = "peer-a"
        )
        assertNull(restored.selectedDeviceId)

        val discovered = TransferUiReducer.withDevices(restored, listOf(peerA))

        assertEquals("peer-a", discovered.selectedDeviceId)
        assertEquals(listOf(selectedFile), discovered.selectedFiles)
    }

    @Test
    fun `manual peer selection clears pending history preference`() {
        val restored = TransferUiReducer.restoreHistoryFile(
            TransferUiState(devices = listOf(peerA, peerB)), selectedFile, "peer-a"
        )

        val selected = TransferUiReducer.selectDevice(restored, "peer-b")

        assertEquals("peer-b", selected.selectedDeviceId)
        assertNull(selected.preferredDeviceId)
    }

    @Test
    fun `invalid selection does not persist uri permission`() {
        val events = mutableListOf<String>()

        val result = validateThenPersist(
            validate = {
                events += "validate"
                null
            },
            persist = { events += "persist" }
        )

        assertNull(result)
        assertEquals(listOf("validate"), events)
    }

    @Test
    fun `valid selection persists only after validation`() {
        val events = mutableListOf<String>()

        val result = validateThenPersist(
            validate = {
                events += "validate"
                "selected"
            },
            persist = { events += "persist" }
        )

        assertEquals("selected", result)
        assertEquals(listOf("validate", "persist"), events)
    }

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

    @Test
    fun `service state preserves notice without changing pause controls`() {
        val current = TransferUiState(notice = "2 个文件无法读取，已跳过")
        val service = ServiceTransferState(transfer = ServiceTransfer(
            "发送", "b.txt", 40, "已暂停", true,
            fileIndex = 2, fileCount = 3, batchProgress = 50,
            pauseState = TransferPauseState.PAUSED
        ))

        val result = TransferUiReducer.withServiceState(current, service)

        assertEquals("2 个文件无法读取，已跳过", result.notice)
        assertFalse(result.canPause)
        assertTrue(result.canResume)
    }

    @Test
    fun `selecting files replaces both selection and notice atomically`() {
        val replacement = listOf(SelectedFile("content://new", "new.txt", "text/plain", 2))
        val current = TransferUiState(
            selectedFiles = listOf(SelectedFile("content://old", "old.txt", "text/plain", 1)),
            notice = "old notice"
        )

        val result = TransferUiReducer.selectFiles(current, replacement, "new notice")

        assertEquals(replacement, result.selectedFiles)
        assertEquals("new notice", result.notice)
    }

    @Test
    fun `successful file selection clears previous notice`() {
        val current = TransferUiState(notice = "old notice")
        val files = listOf(SelectedFile("content://new", "new.txt", "text/plain", 2))

        val result = TransferUiReducer.selectFiles(current, files)

        assertEquals(files, result.selectedFiles)
        assertNull(result.notice)
    }

    @Test
    fun `only the latest selection request token can commit`() {
        val requests = LatestSelectionRequest()

        val token1 = requests.nextToken()
        val token2 = requests.nextToken()

        assertFalse(requests.isLatest(token1))
        assertTrue(requests.isLatest(token2))
    }
}
