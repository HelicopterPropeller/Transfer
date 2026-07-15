package com.example.transfer.protocol

import org.junit.Assert.assertEquals
import org.junit.Test

class TransferProtocolTest {
    @Test
    fun `v4 transfer bounds are fixed`() {
        assertEquals(4, ConnectionProtocol.VERSION)
        assertEquals(1_048_576, TransferProtocol.CHUNK_SIZE)
        assertEquals(10L * 1024 * 1024 * 1024, TransferProtocol.MAX_FILE_SIZE)
    }
}
