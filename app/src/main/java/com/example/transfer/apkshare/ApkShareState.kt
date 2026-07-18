package com.example.transfer.apkshare

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

enum class RetryAction {
    PREPARE_EXISTING_LAN,
    PREPARE_HOTSPOT,
    RESOLVE_MANUAL_HOTSPOT,
}

sealed interface ApkShareState {
    data object Idle : ApkShareState
    data object PreparingApk : ApkShareState
    data class PermissionRequired(val requirement: HotspotRequirement) : ApkShareState
    data object StartingHotspot : ApkShareState
    data class JoinHotspot(val credentials: HotspotCredentials) : ApkShareState
    data class ReadyToDownload(
        val url: String,
        val artifact: ApkArtifact,
        val expiresAtMillis: Long,
    ) : ApkShareState

    data class Downloading(val bytesSent: Long, val totalBytes: Long) : ApkShareState {
        val progressPercent: Int = if (totalBytes <= 0L) {
            0
        } else {
            ((bytesSent.coerceIn(0L, totalBytes).toDouble() / totalBytes.toDouble()) * 100.0)
                .toInt()
        }
    }

    data object Completed : ApkShareState
    data class ManualHotspotRequired(val reason: HotspotFailure) : ApkShareState
    data class Error(val message: String, val retry: RetryAction?) : ApkShareState
    data object Cancelled : ApkShareState
}

internal class ApkShareCoordinator(
    private val closeServer: () -> Unit,
    private val closeHotspot: () -> Unit,
    private val deleteArtifact: () -> Unit,
) : Closeable {
    private val closed = AtomicBoolean()

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        runCatching(closeServer)
        runCatching(closeHotspot)
        runCatching(deleteArtifact)
    }
}
