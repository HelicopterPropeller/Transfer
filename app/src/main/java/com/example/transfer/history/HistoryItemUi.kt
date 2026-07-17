package com.example.transfer.history

import android.os.Build
import androidx.annotation.RequiresApi
import java.time.ZoneId
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone

data class HistoryItemUi(
    val id: Long,
    val fileName: String,
    val fileSize: Long,
    val mimeType: String,
    val directionText: String,
    val statusText: String,
    val peerId: String?,
    val peerText: String,
    val fileSizeText: String,
    val metadataText: String,
    val startedText: String,
    val errorText: String?,
    val sourceUri: String?,
    val receivedUri: String?,
    val showResend: Boolean,
    val showOpen: Boolean,
    val isActiveOutgoing: Boolean
) {
    companion object {
        @RequiresApi(Build.VERSION_CODES.O)
        fun from(
            entry: TransferHistoryEntry,
            locale: Locale,
            zoneId: ZoneId
        ): HistoryItemUi = from(entry, locale, TimeZone.getTimeZone(zoneId.id))

        fun from(
            entry: TransferHistoryEntry,
            locale: Locale,
            timeZone: TimeZone
        ): HistoryItemUi {
            val peer = entry.peerName?.takeIf(String::isNotBlank)
                ?: entry.peerAddress?.takeIf(String::isNotBlank)
                ?: "未知设备"
            val size = formatBytes(entry.fileSize, locale)
            return HistoryItemUi(
                id = entry.id,
                fileName = entry.fileName,
                fileSize = entry.fileSize,
                mimeType = entry.mimeType,
                directionText = when (entry.direction) {
                    TransferDirection.SEND -> "发送"
                    TransferDirection.RECEIVE -> "接收"
                },
                statusText = when (entry.status) {
                    TransferHistoryStatus.IN_PROGRESS -> "进行中"
                    TransferHistoryStatus.SUCCESS -> "已完成"
                    TransferHistoryStatus.FAILED -> "失败"
                    TransferHistoryStatus.CANCELLED -> "已取消"
                    TransferHistoryStatus.INTERRUPTED -> "已中断"
                },
                peerId = entry.peerId,
                peerText = peer,
                fileSizeText = size,
                metadataText = "$peer · $size",
                startedText = SimpleDateFormat("yyyy-MM-dd HH:mm", locale).apply {
                    this.timeZone = timeZone
                }.format(Date(entry.startedAt)),
                errorText = entry.errorMessage.toBoundedError(),
                sourceUri = entry.sourceUri,
                receivedUri = entry.receivedUri,
                showResend = entry.direction == TransferDirection.SEND &&
                    !entry.sourceUri.isNullOrBlank(),
                showOpen = entry.direction == TransferDirection.RECEIVE &&
                    entry.status == TransferHistoryStatus.SUCCESS &&
                    !entry.receivedUri.isNullOrBlank(),
                isActiveOutgoing = entry.direction == TransferDirection.SEND &&
                    entry.status == TransferHistoryStatus.IN_PROGRESS
            )
        }

        private fun formatBytes(bytes: Long, locale: Locale): String {
            val safeBytes = bytes.coerceAtLeast(0)
            val units = listOf(
                1L to "B",
                1_024L to "KB",
                1_024L * 1_024 to "MB",
                1_024L * 1_024 * 1_024 to "GB"
            )
            val (unitSize, label) = units.lastOrNull { (size, _) -> safeBytes >= size }
                ?: units.first()
            return if (unitSize == 1L) {
                "$safeBytes B"
            } else {
                String.format(locale, "%.1f %s", safeBytes.toDouble() / unitSize, label)
            }
        }

        private fun String?.toBoundedError(): String? {
            val message = this?.trim()?.takeIf(String::isNotBlank) ?: return null
            return if (message.length <= MAX_ERROR_TEXT_LENGTH) {
                message
            } else {
                message.take(MAX_ERROR_TEXT_LENGTH - 1) + "…"
            }
        }

        private const val MAX_ERROR_TEXT_LENGTH = 200
    }
}
