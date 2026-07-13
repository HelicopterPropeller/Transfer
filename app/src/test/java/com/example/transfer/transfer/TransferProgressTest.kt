package com.example.transfer.transfer

import org.junit.Assert.assertEquals
import org.junit.Test

class TransferProgressTest {
    @Test
    fun `progress handles boundaries`() {
        assertEquals(100, TransferProgress.percent(0, 0))
        assertEquals(50, TransferProgress.percent(5, 10))
        assertEquals(100, TransferProgress.percent(20, 10))
        assertEquals(0, TransferProgress.percent(-1, 10))
    }
}
