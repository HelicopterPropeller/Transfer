package com.example.transfer.service

import com.example.transfer.transfer.TransferPauseState
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class TransferNotificationModelTest {
    @Test
    fun `idle state shows waiting notification`() {
        val model = TransferNotificationModel.from(ServiceTransferState())
        assertEquals("局域网互传运行中", model.title)
        assertEquals("等待设备或文件", model.text)
        assertFalse(model.showProgress)
    }

    @Test
    fun `active state shows direction file and progress`() {
        val state = ServiceTransferState(transfer = ServiceTransfer("发送", "movie.mp4", 40, "正在发送", true))
        val model = TransferNotificationModel.from(state)
        assertEquals("发送文件", model.title)
        assertEquals("movie.mp4 · 40%", model.text)
        assertTrue(model.showProgress)
        assertEquals(40, model.progress)
    }

    @Test
    fun `finished state displays result without progress`() {
        val state = ServiceTransferState(transfer = ServiceTransfer("接收", "a.zip", 100, "接收完成", false))
        val model = TransferNotificationModel.from(state)
        assertEquals("接收完成", model.title)
        assertEquals("a.zip", model.text)
        assertFalse(model.showProgress)
    }

    @Test
    fun `running batch offers pause with queue progress`() {
        val transfer = ServiceTransfer(
            "发送", "b.bin", 25, "正在发送", true,
            fileIndex = 2, fileCount = 3, batchProgress = 50,
            pauseState = TransferPauseState.RUNNING
        )

        val model = TransferNotificationModel.from(ServiceTransferState(transfer = transfer))

        assertEquals("第 2/3 个 · b.bin · 50%", model.text)
        assertEquals(TransferNotificationAction.PAUSE, model.action)
    }

    @Test
    fun `paused outgoing transfer offers resume`() {
        val transfer = ServiceTransfer(
            "发送", "b.bin", 25, "已暂停", true,
            pauseState = TransferPauseState.PAUSED
        )

        assertEquals(
            TransferNotificationAction.RESUME,
            TransferNotificationModel.from(ServiceTransferState(transfer = transfer)).action
        )
    }

    @Test
    fun `pausing outgoing transfer offers resume`() {
        val transfer = ServiceTransfer(
            "发送", "b.bin", 25, "正在暂停", true,
            pauseState = TransferPauseState.PAUSING
        )

        assertEquals(
            TransferNotificationAction.RESUME,
            TransferNotificationModel.from(ServiceTransferState(transfer = transfer)).action
        )
    }

    @Test
    fun `termination gate rejects publishing after close`() {
        val gate = ServiceTerminationGate()
        var publications = 0

        assertTrue(gate.runIfOpen { publications++ })
        gate.close()

        assertFalse(gate.runIfOpen { publications++ })
        assertEquals(1, publications)
    }

    @Test
    fun `termination gate close is idempotent`() {
        val gate = ServiceTerminationGate()

        assertTrue(gate.close())
        assertFalse(gate.close())
    }

    @Test
    fun `terminal action runs once and closes gate`() {
        val gate = ServiceTerminationGate()
        var terminalActions = 0
        var laterActions = 0

        assertTrue(gate.closeWithAction { terminalActions++ })
        assertFalse(gate.closeWithAction { terminalActions++ })
        assertFalse(gate.runIfOpen { laterActions++ })

        assertEquals(1, terminalActions)
        assertEquals(0, laterActions)
    }

    @Test
    fun `checked batch length rejects total overflow`() {
        assertNull(checkedBatchLength(listOf(Long.MAX_VALUE, 1L)))
    }

    @Test
    fun `late pause publication reads latest controller state`() {
        val initial = ServiceTransferState(transfer = ServiceTransfer(
            "发送", "b.bin", 25, "正在暂停", true,
            pauseState = TransferPauseState.PAUSING
        ))
        var latestState = TransferPauseState.PAUSING
        val reduce: (ServiceTransferState) -> ServiceTransferState = { state ->
            state.withLatestOutgoingPauseState(
                controllerMatches = true,
                latestPauseState = { latestState }
            )
        }

        latestState = TransferPauseState.PAUSED
        val reduced = reduce(initial)

        assertEquals(TransferPauseState.PAUSED, reduced.transfer?.pauseState)
        assertEquals("已暂停", reduced.transfer?.message)
    }

    @Test
    fun `batch level failure becomes inactive`() {
        val active = ServiceTransferState(transfer = ServiceTransfer(
            "发送", "b.bin", 25, "正在发送", true
        ))

        val failed = active.withInactiveBatchFailure("批次失败")

        assertFalse(failed.transfer?.active ?: true)
        assertEquals("批次失败", failed.transfer?.message)
        assertEquals(
            TransferNotificationAction.NONE,
            TransferNotificationModel.from(failed).action
        )
    }
}
