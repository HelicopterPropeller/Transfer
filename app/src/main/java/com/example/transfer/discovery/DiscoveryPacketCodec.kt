package com.example.transfer.discovery

object DiscoveryPacketCodec {
    const val MAX_PACKET_BYTES = 2048
    private const val VERSION = 3
    private val packetPattern = Regex(
        """^\{"protocol":"transfer-mvp","version":(\d+),"id":"((?:\\.|[^"\\])*)","name":"((?:\\.|[^"\\])*)","port":(\d+)\}$"""
    )

    fun encode(id: String, name: String, port: Int): ByteArray {
        require(id.isNotBlank() && name.isNotBlank() && port in 1..65535)
        val json = """{"protocol":"transfer-mvp","version":$VERSION,"id":"${escape(id)}","name":"${escape(name)}","port":$port}"""
        return json.toByteArray(Charsets.UTF_8).also {
            require(it.size <= MAX_PACKET_BYTES) { "发现报文过长" }
        }
    }

    fun decode(bytes: ByteArray, length: Int): DiscoveryPacket? {
        if (length !in 1..minOf(bytes.size, MAX_PACKET_BYTES)) return null
        val match = packetPattern.matchEntire(bytes.decodeToString(0, length)) ?: return null
        if (match.groupValues[1].toIntOrNull() != VERSION) return null
        val id = unescape(match.groupValues[2])?.takeIf(String::isNotBlank) ?: return null
        val name = unescape(match.groupValues[3])?.takeIf(String::isNotBlank) ?: return null
        val port = match.groupValues[4].toIntOrNull()?.takeIf { it in 1..65535 } ?: return null
        return DiscoveryPacket(id, name, port)
    }

    private fun escape(value: String): String = buildString {
        value.forEach { character ->
            when (character) {
                '"' -> append("\\\"")
                '\\' -> append("\\\\")
                '\b' -> append("\\b")
                '\u000C' -> append("\\f")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else -> if (character.code < 0x20) append("\\u%04x".format(character.code)) else append(character)
            }
        }
    }

    private fun unescape(value: String): String? = runCatching {
        buildString {
            var index = 0
            while (index < value.length) {
                val character = value[index++]
                if (character != '\\') {
                    append(character)
                    continue
                }
                check(index < value.length)
                when (val escaped = value[index++]) {
                    '"', '\\', '/' -> append(escaped)
                    'b' -> append('\b')
                    'f' -> append('\u000C')
                    'n' -> append('\n')
                    'r' -> append('\r')
                    't' -> append('\t')
                    'u' -> {
                        check(index + 4 <= value.length)
                        append(value.substring(index, index + 4).toInt(16).toChar())
                        index += 4
                    }
                    else -> error("非法 JSON 转义")
                }
            }
        }
    }.getOrNull()
}
