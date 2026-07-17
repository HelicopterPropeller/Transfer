package com.example.transfer.pairing

import org.junit.Assert.assertEquals
import org.junit.Test

class QrDisplaySizerTest {
    @Test
    fun `narrow content uses all available width`() {
        assertEquals(216, QrDisplaySizer.side(216, 840, 600))
    }

    @Test
    fun `wide content is capped and missing measurement uses bounded fallback`() {
        assertEquals(840, QrDisplaySizer.side(1_200, 840, 600))
        assertEquals(600, QrDisplaySizer.side(0, 840, 600))
        assertEquals(400, QrDisplaySizer.side(0, 400, 600))
    }
}
