package com.example.transfer.apkshare

import org.junit.Assert.assertEquals
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
}
