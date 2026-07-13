package com.example.transfer.storage

object FileNamePolicy {
    private const val MAX_LENGTH = 180

    fun sanitize(input: String): String {
        var name = input
            .filterNot(Char::isISOControl)
            .replace('/', '_')
            .replace('\\', '_')
            .trim()
            .trimStart('.', '_', ' ')
        while (name.contains("..")) name = name.replace("..", ".")
        if (name.isBlank() || name == ".") return "received_file"
        if (name.length <= MAX_LENGTH) return name
        val dot = name.lastIndexOf('.').takeIf { it in 1 until name.lastIndex } ?: -1
        if (dot < 0) return name.take(MAX_LENGTH)
        val extension = name.substring(dot).take(20)
        return name.substring(0, dot).take(MAX_LENGTH - extension.length) + extension
    }

    fun withTimestamp(name: String, timestamp: String, attempt: Int): String {
        val safeName = sanitize(name)
        val dot = safeName.lastIndexOf('.').takeIf { it > 0 } ?: safeName.length
        val base = safeName.substring(0, dot)
        val extension = safeName.substring(dot)
        val attemptSuffix = if (attempt > 1) "_$attempt" else ""
        val suffix = "_${timestamp}${attemptSuffix}"
        val availableBase = (MAX_LENGTH - extension.length - suffix.length).coerceAtLeast(1)
        return base.take(availableBase) + suffix + extension
    }
}
