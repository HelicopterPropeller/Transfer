package com.example.transfer.apkshare

import androidx.annotation.StringRes
import com.example.transfer.R
import java.net.URI

enum class ApkSharePrimaryAction {
    START_EXISTING_LAN,
    START_HOTSPOT,
    SHOW_DOWNLOAD_QR,
    OPEN_HOTSPOT_SETTINGS,
    RETRY,
}

data class ApkShareUiModel(
    @StringRes val titleRes: Int,
    @StringRes val detailRes: Int,
    val detailArgs: List<Any> = emptyList(),
    val qrPayload: String? = null,
    val primaryAction: ApkSharePrimaryAction? = null,
    val secondaryAction: ApkSharePrimaryAction? = null,
    val showCredentials: Boolean = false,
    val showArtifactMetadata: Boolean = false,
    val progress: Int? = null,
    val credentials: HotspotCredentials? = null,
    val artifact: ApkArtifact? = null,
    val shortAddress: String? = null,
    val remainingSeconds: Long? = null,
    val hotspotFailure: HotspotFailure? = null,
    val permissionRequirement: HotspotRequirement? = null,
    val retryAction: RetryAction? = null,
) {
    companion object {
        fun from(state: ApkShareState, nowMillis: Long): ApkShareUiModel = when (state) {
            ApkShareState.Idle -> ApkShareUiModel(
                titleRes = R.string.apk_share_idle_title,
                detailRes = R.string.apk_share_idle_detail,
                primaryAction = ApkSharePrimaryAction.START_EXISTING_LAN,
                secondaryAction = ApkSharePrimaryAction.START_HOTSPOT,
            )

            ApkShareState.PreparingApk -> ApkShareUiModel(
                titleRes = R.string.apk_share_preparing_title,
                detailRes = R.string.apk_share_preparing_detail,
            )

            is ApkShareState.PermissionRequired -> ApkShareUiModel(
                titleRes = R.string.apk_share_permission_title,
                detailRes = R.string.apk_share_permission_detail,
                primaryAction = ApkSharePrimaryAction.START_HOTSPOT,
                permissionRequirement = state.requirement,
            )

            ApkShareState.StartingHotspot -> ApkShareUiModel(
                titleRes = R.string.apk_share_starting_hotspot_title,
                detailRes = R.string.apk_share_starting_hotspot_detail,
            )

            is ApkShareState.JoinHotspot -> ApkShareUiModel(
                titleRes = R.string.apk_share_join_hotspot_title,
                detailRes = R.string.apk_share_join_hotspot_detail,
                detailArgs = listOf(state.credentials.ssid),
                qrPayload = state.credentials.wifiQrPayload,
                primaryAction = ApkSharePrimaryAction.SHOW_DOWNLOAD_QR,
                showCredentials = true,
                credentials = state.credentials,
            )

            is ApkShareState.ReadyToDownload -> {
                val remaining = (state.expiresAtMillis - nowMillis).coerceAtLeast(0L)
                val seconds = (remaining + 999L) / 1_000L
                val address = runCatching { URI(state.url).authority }
                    .getOrNull()
                    ?.takeIf(String::isNotBlank)
                    ?: state.url
                ApkShareUiModel(
                    titleRes = R.string.apk_share_ready_title,
                    detailRes = R.string.apk_share_ready_detail,
                    detailArgs = listOf(address, seconds),
                    qrPayload = state.url,
                    showArtifactMetadata = true,
                    artifact = state.artifact,
                    shortAddress = address,
                    remainingSeconds = seconds,
                )
            }

            is ApkShareState.Downloading -> ApkShareUiModel(
                titleRes = R.string.apk_share_downloading_title,
                detailRes = R.string.apk_share_downloading_detail,
                detailArgs = listOf(state.progressPercent),
                progress = state.progressPercent,
            )

            ApkShareState.Completed -> ApkShareUiModel(
                titleRes = R.string.apk_share_completed_title,
                detailRes = R.string.apk_share_completed_detail,
            )

            is ApkShareState.ManualHotspotRequired -> ApkShareUiModel(
                titleRes = R.string.apk_share_manual_hotspot_title,
                detailRes = R.string.apk_share_manual_hotspot_detail,
                primaryAction = ApkSharePrimaryAction.OPEN_HOTSPOT_SETTINGS,
                hotspotFailure = state.reason,
            )

            is ApkShareState.Error -> ApkShareUiModel(
                titleRes = R.string.apk_share_error_title,
                detailRes = R.string.apk_share_error_detail,
                detailArgs = listOf(state.message),
                primaryAction = state.retry?.let { ApkSharePrimaryAction.RETRY },
                retryAction = state.retry,
            )

            ApkShareState.Cancelled -> ApkShareUiModel(
                titleRes = R.string.apk_share_cancelled_title,
                detailRes = R.string.apk_share_cancelled_detail,
            )
        }
    }
}
