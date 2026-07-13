package com.example.transfer.ui

import com.example.transfer.discovery.DiscoveredDevice
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
    fun `send is enabled only with live peer file and no active transfer`() {
        var state = TransferUiState()
        assertFalse(state.canSend)

        state = TransferUiReducer.withDevices(state, listOf(peer))
        state = TransferUiReducer.selectDevice(state, "peer")
        state = state.copy(selectedFile = SelectedFile("content://file", "a.txt", "text/plain", 4))
        assertTrue(state.canSend)

        state = state.copy(transfer = TransferStatus("发送", "a.txt", 10, "发送中", true))
        assertFalse(state.canSend)
    }

    @Test
    fun `expired selected peer is cleared`() {
        var state = TransferUiState(devices = listOf(peer), selectedDeviceId = "peer")
        state = TransferUiReducer.withDevices(state, emptyList())
        assertNull(state.selectedDeviceId)
    }
}
