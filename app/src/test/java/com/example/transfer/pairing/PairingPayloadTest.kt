package com.example.transfer.pairing

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class PairingPayloadTest {
    @Test
    fun `pairing payload round trips reserved characters`() {
        val payload = PairingPayload(
            version = 4,
            deviceId = "device-1&x",
            deviceName = "Pixel 9 & \u6d4b\u8bd5",
            ip = "192.168.1.5",
            port = 42043,
            token = "AQID+/=",
            expiresAt = 123_456_789L
        )

        assertEquals(
            payload,
            PairingPayloadCodec.decode(PairingPayloadCodec.encode(payload), nowMillis = 1L)
        )
    }

    @Test
    fun `codec rejects expired public and oversized payloads`() {
        assertRejected(
            "lantransfer://pair?v=4&id=a&name=b&ip=8.8.8.8&port=42043&token=x&expires=10",
            now = 1L
        )
        assertRejected(validRaw(expires = 10L), now = 10L)
        assertRejected("x".repeat(PairingPayloadCodec.MAX_PAYLOAD_CHARS + 1), now = 0L)
    }

    @Test
    fun `codec rejects wrong location extra URI parts and malformed fields`() {
        assertRejected(validRaw().replace("lantransfer", "https"))
        assertRejected(validRaw().replace("//pair", "//other"))
        assertRejected(validRaw().replace("//pair?", "//pair/path?"))
        assertRejected(validRaw() + "#fragment")
        assertRejected(validRaw().replace("//pair", "//user@pair"))
        assertRejected(validRaw() + "&id=duplicate")
        assertRejected(validRaw().replace("&token=token", ""))
        assertRejected(validRaw().replace("v=4", "v=3"))
        assertRejected(validRaw().replace("port=42043", "port=0"))
        assertRejected(validRaw().replace("port=42043", "port=65536"))
        assertRejected(validRaw().replace("id=device", "id=%00"))
    }

    @Test
    fun `codec accepts only canonical private ipv4 addresses`() {
        listOf("10.0.0.1", "172.16.0.1", "172.31.255.254", "192.168.0.1").forEach { ip ->
            val decoded = PairingPayloadCodec.decode(validRaw(ip = ip), nowMillis = 1L)
            assertEquals(ip, decoded.ip)
        }
        listOf(
            "0.0.0.0", "127.0.0.1", "169.254.1.1", "172.15.0.1", "172.32.0.1",
            "192.0.2.1", "224.0.0.1", "255.255.255.255", "192.168.001.1", "::1"
        ).forEach { ip -> assertRejected(validRaw(ip = ip)) }
    }

    @Test
    fun `codec enforces utf8 byte limits and nonblank fields`() {
        assertThrows(PairingPayloadException::class.java) {
            PairingPayloadCodec.encode(payload(deviceId = "a".repeat(65)))
        }
        assertThrows(PairingPayloadException::class.java) {
            PairingPayloadCodec.encode(payload(deviceName = "\u6d4b".repeat(43)))
        }
        assertThrows(PairingPayloadException::class.java) {
            PairingPayloadCodec.encode(payload(token = "t".repeat(129)))
        }
        assertThrows(PairingPayloadException::class.java) {
            PairingPayloadCodec.encode(payload(deviceName = "   "))
        }
    }

    private fun assertRejected(raw: String, now: Long = 1L) {
        assertThrows(PairingPayloadException::class.java) {
            PairingPayloadCodec.decode(raw, nowMillis = now)
        }
    }

    private fun validRaw(ip: String = "192.168.1.5", expires: Long = 100L) =
        "lantransfer://pair?v=4&id=device&name=Pixel&ip=$ip&port=42043&token=token&expires=$expires"

    private fun payload(
        deviceId: String = "device",
        deviceName: String = "Pixel",
        token: String = "token"
    ) = PairingPayload(4, deviceId, deviceName, "192.168.1.5", 42043, token, 100L)
}
