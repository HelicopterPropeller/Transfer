package com.example.transfer.apkshare

import java.io.File
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class ApkShareCoordinatorTest {
    @Test
    fun `coordinator closes server hotspot and artifact once in order`() {
        val events = mutableListOf<String>()
        val coordinator = ApkShareCoordinator(
            closeServer = { events += "server" },
            closeHotspot = { events += "hotspot" },
            deleteArtifact = { events += "artifact" },
        )

        coordinator.close()
        coordinator.close()

        assertEquals(listOf("server", "hotspot", "artifact"), events)
    }

    @Test
    fun `coordinator attempts every cleanup when an earlier cleanup fails`() {
        val events = mutableListOf<String>()
        val coordinator = ApkShareCoordinator(
            closeServer = {
                events += "server"
                error("server close failed")
            },
            closeHotspot = {
                events += "hotspot"
                error("hotspot close failed")
            },
            deleteArtifact = { events += "artifact" },
        )

        coordinator.close()

        assertEquals(listOf("server", "hotspot", "artifact"), events)
    }

    @Test
    fun `interrupted download returns to the same ready session without completing cleanup`() {
        val ready = ApkShareState.ReadyToDownload(
            url = "http://192.168.1.2:1234/i/token/",
            artifact = ApkArtifact(File("unused.apk"), "app.apk", "1.0", 1L, 100L, "hash"),
            expiresAtMillis = 60_000L,
        )
        val published = mutableListOf<ApkShareState>()
        var completedCleanup = false
        val listener = RetryableApkDownloadListener(
            readyState = { ready },
            publish = { state -> published += state; true },
            complete = { false },
            onCompletedPublished = { completedCleanup = true },
        )

        listener.onStarted(100L)
        listener.onProgress(25L, 100L)
        listener.onFailed("browser disconnected")

        assertEquals(ApkShareState.Downloading(0L, 100L), published[0])
        assertEquals(ApkShareState.Downloading(25L, 100L), published[1])
        assertSame(ready, published[2])
        assertFalse(completedCleanup)
    }

    @Test
    fun `completed download invokes terminal service cleanup only after state is published`() {
        val events = mutableListOf<String>()
        val listener = RetryableApkDownloadListener(
            readyState = { null },
            publish = { true },
            complete = { events += "Completed"; true },
            onCompletedPublished = { events += "stop" },
        )

        listener.onCompleted()

        assertEquals(listOf("Completed", "stop"), events)
    }

    @Test
    fun `leaving UI stops only a truly idle service`() {
        assertTrue(ApkShareServiceStopPolicy.shouldStopOnUiExit(ApkShareState.Idle, false))
        assertFalse(ApkShareServiceStopPolicy.shouldStopOnUiExit(ApkShareState.Idle, true))
        assertFalse(
            ApkShareServiceStopPolicy.shouldStopOnUiExit(ApkShareState.PreparingApk, true),
        )
        assertFalse(
            ApkShareServiceStopPolicy.shouldStopOnUiExit(ApkShareState.Completed, false),
        )
        assertTrue(
            ApkShareServiceStopPolicy.shouldStopAfterCompletion(ApkShareState.Completed, false),
        )
        assertFalse(
            ApkShareServiceStopPolicy.shouldStopAfterCompletion(ApkShareState.Completed, true),
        )
        assertFalse(
            ApkShareServiceStopPolicy.shouldStopAfterCompletion(ApkShareState.Cancelled, false),
        )
    }
}
