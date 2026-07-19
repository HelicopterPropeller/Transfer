package com.example.transfer.apkshare

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class ApkShareStateTest {
    @get:Rule
    val temporary = TemporaryFolder()

    @Test
    fun `downloading state clamps progress without integer overflow`() {
        assertEquals(
            100,
            ApkShareState.Downloading(Long.MAX_VALUE, Long.MAX_VALUE).progressPercent,
        )
        assertEquals(0, ApkShareState.Downloading(-1L, 100L).progressPercent)
        assertEquals(100, ApkShareState.Downloading(101L, 100L).progressPercent)
        assertEquals(0, ApkShareState.Downloading(1L, 0L).progressPercent)
    }

    @Test
    fun `downloading state reports stable intermediate percentage`() {
        assertEquals(25, ApkShareState.Downloading(1L, 4L).progressPercent)
        assertEquals(66, ApkShareState.Downloading(2L, 3L).progressPercent)
    }

    @Test
    fun `notification gate coalesces pending updates and rejects work after close`() {
        val gate = ApkShareNotificationGate()

        assertTrue(gate.request())
        assertFalse(gate.request())
        assertTrue(gate.beginDelivery())
        assertTrue(gate.request())

        gate.close()

        assertFalse(gate.beginDelivery())
        assertFalse(gate.request())
    }

    @Test
    fun `preparation workspace removes partial apk recursively`() {
        val directory = temporary.newFolder("generation")
        val nested = directory.resolve("nested").apply { mkdirs() }
        nested.resolve("partial.apk").writeText("partial")

        ApkPreparationWorkspace(directory).clear()

        assertFalse(directory.exists())
    }
}
