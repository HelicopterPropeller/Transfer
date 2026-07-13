package com.example.transfer.discovery

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class DiscoveryPacketCodecTest {
    @Test
    fun `packet round trips escaped device name`() {
        val encoded = DiscoveryPacketCodec.encode("id-1", "Pixel \"Lab\"\\A", 42043)
        assertEquals(
            DiscoveryPacket("id-1", "Pixel \"Lab\"\\A", 42043),
            DiscoveryPacketCodec.decode(encoded, encoded.size)
        )
    }

    @Test
    fun `unknown version is rejected`() {
        val bytes = """{"protocol":"transfer-mvp","version":2,"id":"a","name":"b","port":42043}"""
            .toByteArray()
        assertNull(DiscoveryPacketCodec.decode(bytes, bytes.size))
    }

    @Test
    fun `malformed and oversized packets are rejected`() {
        assertNull(DiscoveryPacketCodec.decode("not-json".toByteArray(), 8))
        val oversized = ByteArray(DiscoveryPacketCodec.MAX_PACKET_BYTES + 1) { 'a'.code.toByte() }
        assertNull(DiscoveryPacketCodec.decode(oversized, oversized.size))
    }

    @Test
    fun `invalid port is rejected`() {
        val bytes = """{"protocol":"transfer-mvp","version":1,"id":"a","name":"b","port":0}"""
            .toByteArray()
        assertNull(DiscoveryPacketCodec.decode(bytes, bytes.size))
    }
}
