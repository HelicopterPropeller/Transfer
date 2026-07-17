package com.example.transfer.pairing

import java.net.Inet4Address
import java.net.InetAddress
import java.net.URI
import java.net.URLDecoder
import java.net.URLEncoder

data class PairingPayload(
    val version: Int,
    val deviceId: String,
    val deviceName: String,
    val ip: String,
    val port: Int,
    val token: String,
    val expiresAt: Long
)

class PairingPayloadException(message: String) : IllegalArgumentException(message)

object PairingPayloadCodec {
    const val MAX_PAYLOAD_CHARS = 1024
    const val MAX_DEVICE_ID_BYTES = 64
    const val MAX_DEVICE_NAME_BYTES = 128
    const val MAX_TOKEN_BYTES = 128
    private val fields = setOf("v", "id", "name", "ip", "port", "token", "expires")

    fun encode(payload: PairingPayload): String {
        validate(payload, nowMillis = null)
        fun encoded(value: String): String = URLEncoder
            .encode(value, Charsets.UTF_8.name())
            .replace("+", "%20")
        val raw = "lantransfer://pair?v=${payload.version}" +
            "&id=${encoded(payload.deviceId)}" +
            "&name=${encoded(payload.deviceName)}" +
            "&ip=${encoded(payload.ip)}" +
            "&port=${payload.port}" +
            "&token=${encoded(payload.token)}" +
            "&expires=${payload.expiresAt}"
        if (raw.length > MAX_PAYLOAD_CHARS) fail("Pairing payload is too long")
        return raw
    }

    fun decode(raw: String, nowMillis: Long): PairingPayload {
        if (raw.length > MAX_PAYLOAD_CHARS) fail("Pairing payload is too long")
        val uri = try {
            URI(raw)
        } catch (_: Exception) {
            fail("Malformed pairing payload")
        }
        if (uri.scheme != "lantransfer" || uri.host != "pair") {
            fail("Not a Transfer pairing payload")
        }
        if (uri.userInfo != null || uri.port != -1 || uri.path.orEmpty().isNotEmpty() ||
            uri.fragment != null
        ) fail("Invalid pairing URI")

        val pairs = try {
            uri.rawQuery.orEmpty().split('&').map { part ->
                val separator = part.indexOf('=')
                if (separator <= 0) fail("Invalid pairing field")
                URLDecoder.decode(part.substring(0, separator), Charsets.UTF_8.name()) to
                    URLDecoder.decode(part.substring(separator + 1), Charsets.UTF_8.name())
            }
        } catch (error: PairingPayloadException) {
            throw error
        } catch (_: Exception) {
            fail("Invalid pairing field encoding")
        }
        val values = pairs.groupBy({ it.first }, { it.second })
        if (values.keys != fields || values.values.any { it.size != 1 }) {
            fail("Pairing fields are incomplete")
        }
        fun one(key: String) = values.getValue(key).single()
        val payload = PairingPayload(
            version = one("v").toIntOrNull() ?: fail("Invalid protocol version"),
            deviceId = one("id"),
            deviceName = one("name"),
            ip = one("ip"),
            port = one("port").toIntOrNull() ?: fail("Invalid port"),
            token = one("token"),
            expiresAt = one("expires").toLongOrNull() ?: fail("Invalid expiry")
        )
        validate(payload, nowMillis)
        return payload
    }

    private fun validate(payload: PairingPayload, nowMillis: Long?) {
        if (payload.version != 4) fail("Unsupported protocol version")
        if (payload.port !in 1..65535) fail("Invalid port")
        if (payload.expiresAt <= 0L || nowMillis != null && payload.expiresAt <= nowMillis) {
            fail("Pairing payload has expired")
        }
        validateText(payload.deviceId, MAX_DEVICE_ID_BYTES, "Device ID")
        validateText(payload.deviceName, MAX_DEVICE_NAME_BYTES, "Device name")
        validateText(payload.token, MAX_TOKEN_BYTES, "Pairing token")
        if (canonicalPrivateIpv4(payload.ip) == null) fail("Address is not a private IPv4")
    }

    private fun validateText(value: String, maximumBytes: Int, label: String) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        if (value.isBlank() || '\u0000' in value || bytes.size > maximumBytes) {
            fail("$label is invalid")
        }
    }

    private fun fail(message: String): Nothing = throw PairingPayloadException(message)
}

internal fun canonicalPrivateIpv4(raw: String): Inet4Address? {
    val parts = raw.split('.')
    if (parts.size != 4) return null
    val octets = IntArray(4)
    parts.forEachIndexed { index, part ->
        if (part.isEmpty() || part.any { !it.isDigit() } ||
            part.length > 1 && part.startsWith('0')
        ) return null
        octets[index] = part.toIntOrNull()?.takeIf { it in 0..255 } ?: return null
    }
    val privateAddress = octets[0] == 10 ||
        octets[0] == 172 && octets[1] in 16..31 ||
        octets[0] == 192 && octets[1] == 168
    if (!privateAddress) return null
    return InetAddress.getByAddress(ByteArray(4) { octets[it].toByte() }) as Inet4Address
}
