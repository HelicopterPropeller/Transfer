package com.example.transfer.history

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Instant
import java.time.ZoneId
import java.util.Locale
import java.util.TimeZone

class HistoryItemUiTest {
    @Test
    fun `outgoing rows show resend and successful incoming rows show open`() {
        val outgoing = item(
            entry(
                direction = TransferDirection.SEND,
                status = TransferHistoryStatus.FAILED,
                sourceUri = "content://a"
            )
        )
        assertTrue(outgoing.showResend)
        assertFalse(outgoing.showOpen)

        val incoming = item(
            entry(
                direction = TransferDirection.RECEIVE,
                status = TransferHistoryStatus.SUCCESS,
                receivedUri = "content://b"
            )
        )
        assertFalse(incoming.showResend)
        assertTrue(incoming.showOpen)
    }

    @Test
    fun `failed incoming row includes bounded reason and no open action`() {
        val short = item(
            entry(
                direction = TransferDirection.RECEIVE,
                status = TransferHistoryStatus.FAILED,
                errorMessage = "broken"
            )
        )
        val long = item(
            entry(
                status = TransferHistoryStatus.FAILED,
                errorMessage = "x".repeat(500)
            )
        )

        assertEquals("broken", short.errorText)
        assertFalse(short.showOpen)
        assertEquals(200, long.errorText?.length)
        assertTrue(long.errorText?.endsWith("…") == true)
    }

    @Test
    fun `all directions and statuses have Chinese labels`() {
        assertEquals("发送", item(entry(direction = TransferDirection.SEND)).directionText)
        assertEquals("接收", item(entry(direction = TransferDirection.RECEIVE)).directionText)
        assertEquals(
            listOf("进行中", "已完成", "失败", "已取消", "已中断"),
            TransferHistoryStatus.entries.map { status -> item(entry(status = status)).statusText }
        )
    }

    @Test
    fun `file sizes use binary B KB MB and GB units`() {
        assertEquals("0 B", item(entry(fileSize = 0)).fileSizeText)
        assertEquals("900 B", item(entry(fileSize = 900)).fileSizeText)
        assertEquals("1.5 KB", item(entry(fileSize = 1_536)).fileSizeText)
        assertEquals("2.0 MB", item(entry(fileSize = 2L * 1_024 * 1_024)).fileSizeText)
        assertEquals("3.0 GB", item(entry(fileSize = 3L * 1_024 * 1_024 * 1_024)).fileSizeText)
    }

    @Test
    fun `metadata prefers peer name then address then unknown device`() {
        assertEquals("Pixel", item(entry(peerName = "Pixel", peerAddress = "192.168.1.2")).peerText)
        assertEquals("192.168.1.2", item(entry(peerName = "", peerAddress = "192.168.1.2")).peerText)
        assertEquals("未知设备", item(entry(peerName = null, peerAddress = null)).peerText)
    }

    @Test
    fun `started time respects requested zone and blank errors are hidden`() {
        val startedAt = Instant.parse("2024-01-02T03:04:00Z").toEpochMilli()
        val mapped = HistoryItemUi.from(
            entry(startedAt = startedAt, errorMessage = "  "),
            locale = Locale.CHINA,
            zoneId = ZoneId.of("Asia/Shanghai")
        )

        assertEquals("2024-01-02 11:04", mapped.startedText)
        assertNull(mapped.errorText)
    }

    @Test
    fun `legacy time zone path formats without java time runtime`() {
        val startedAt = Instant.parse("2024-01-02T03:04:00Z").toEpochMilli()

        val mapped = HistoryItemUi.from(
            entry(startedAt = startedAt),
            locale = Locale.CHINA,
            timeZone = TimeZone.getTimeZone("Asia/Shanghai")
        )

        assertEquals("2024-01-02 11:04", mapped.startedText)
    }

    private fun item(entry: TransferHistoryEntry) = HistoryItemUi.from(
        entry = entry,
        locale = Locale.US,
        zoneId = ZoneId.of("UTC")
    )

    private fun entry(
        direction: TransferDirection = TransferDirection.SEND,
        status: TransferHistoryStatus = TransferHistoryStatus.SUCCESS,
        fileSize: Long = 4,
        peerName: String? = "Pixel",
        peerAddress: String? = "192.168.1.20",
        startedAt: Long = 0,
        errorMessage: String? = null,
        sourceUri: String? = null,
        receivedUri: String? = null
    ) = TransferHistoryEntry(
        id = 1,
        direction = direction,
        fileName = "a.bin",
        fileSize = fileSize,
        mimeType = "application/octet-stream",
        peerId = "peer-a",
        peerName = peerName,
        peerAddress = peerAddress,
        status = status,
        startedAt = startedAt,
        finishedAt = null,
        errorMessage = errorMessage,
        sourceUri = sourceUri,
        receivedUri = receivedUri
    )
}
