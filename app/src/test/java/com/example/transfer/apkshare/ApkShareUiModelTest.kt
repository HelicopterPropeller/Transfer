package com.example.transfer.apkshare

import java.io.File
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ApkShareUiModelTest {
    @Test
    fun `idle offers existing lan and hotspot choices`() {
        val ui = ApkShareUiModel.from(ApkShareState.Idle, nowMillis = 0L)

        assertEquals(ApkSharePrimaryAction.START_EXISTING_LAN, ui.primaryAction)
        assertEquals(ApkSharePrimaryAction.START_HOTSPOT, ui.secondaryAction)
        assertNull(ui.qrPayload)
    }

    @Test
    fun `join hotspot shows wifi qr credentials and continue action`() {
        val credentials = HotspotCredentials("Transfer", "password", "WIFI:...")

        val ui = ApkShareUiModel.from(ApkShareState.JoinHotspot(credentials), nowMillis = 0L)

        assertEquals("WIFI:...", ui.qrPayload)
        assertEquals(ApkSharePrimaryAction.SHOW_DOWNLOAD_QR, ui.primaryAction)
        assertTrue(ui.showCredentials)
        assertEquals(credentials, ui.credentials)
        assertFalse(ui.showArtifactMetadata)
    }

    @Test
    fun `ready state shows http qr short address metadata and countdown`() {
        val artifact = artifact()
        val state = ApkShareState.ReadyToDownload(
            url = "http://192.168.43.1:42100/i/token/",
            artifact = artifact,
            expiresAtMillis = 10_000L,
        )

        val ui = ApkShareUiModel.from(state, nowMillis = 1_000L)

        assertEquals(state.url, ui.qrPayload)
        assertEquals("192.168.43.1:42100", ui.shortAddress)
        assertEquals(9L, ui.remainingSeconds)
        assertTrue(ui.showArtifactMetadata)
        assertEquals(artifact, ui.artifact)
        assertFalse(ui.showCredentials)
    }

    @Test
    fun `permission and retry states preserve required action context`() {
        val permission = ApkShareUiModel.from(
            ApkShareState.PermissionRequired(HotspotRequirement.NEARBY_WIFI),
            nowMillis = 0L,
        )
        assertEquals(ApkSharePrimaryAction.START_HOTSPOT, permission.primaryAction)
        assertEquals(HotspotRequirement.NEARBY_WIFI, permission.permissionRequirement)

        val error = ApkShareUiModel.from(
            ApkShareState.Error("failed", RetryAction.RESOLVE_MANUAL_HOTSPOT),
            nowMillis = 0L,
        )
        assertEquals(ApkSharePrimaryAction.RETRY, error.primaryAction)
        assertEquals(RetryAction.RESOLVE_MANUAL_HOTSPOT, error.retryAction)
    }

    private fun artifact() = ApkArtifact(
        file = File("Transfer.apk"),
        fileName = "Transfer.apk",
        versionName = "1.0",
        versionCode = 1L,
        size = 1_024L,
        sha256 = "0".repeat(64),
    )
}
