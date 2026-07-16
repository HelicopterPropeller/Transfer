package com.example.transfer.ui

internal enum class SelectedFileProviderKind {
    DOCUMENT,
    MEDIA,
    UNKNOWN
}

internal enum class SelectedFileTimestampColumn {
    DOCUMENT_MILLIS,
    MEDIA_SECONDS
}

internal data class SelectedFileBaseMetadata(
    val displayName: String,
    val size: Long
)

data class SelectedFileMetadata(
    val displayName: String,
    val size: Long,
    val lastModified: Long?
)

internal object SelectedFileMetadataReader {
    fun read(
        kind: SelectedFileProviderKind,
        queryBase: () -> SelectedFileBaseMetadata?,
        queryTimestamp: (SelectedFileTimestampColumn) -> Long?
    ): SelectedFileMetadata? {
        val base = runCatching(queryBase).getOrNull() ?: return null
        val order = when (kind) {
            SelectedFileProviderKind.DOCUMENT -> listOf(
                SelectedFileTimestampColumn.DOCUMENT_MILLIS,
                SelectedFileTimestampColumn.MEDIA_SECONDS
            )
            SelectedFileProviderKind.MEDIA -> listOf(
                SelectedFileTimestampColumn.MEDIA_SECONDS,
                SelectedFileTimestampColumn.DOCUMENT_MILLIS
            )
            SelectedFileProviderKind.UNKNOWN -> listOf(
                SelectedFileTimestampColumn.DOCUMENT_MILLIS,
                SelectedFileTimestampColumn.MEDIA_SECONDS
            )
        }
        val lastModified = order.firstNotNullOfOrNull { column ->
            runCatching { queryTimestamp(column) }.getOrNull()?.let { raw ->
                when (column) {
                    SelectedFileTimestampColumn.DOCUMENT_MILLIS -> raw
                    SelectedFileTimestampColumn.MEDIA_SECONDS -> secondsToMillis(raw)
                }
            }
        }
        return SelectedFileMetadata(base.displayName, base.size, lastModified)
    }

    private fun secondsToMillis(seconds: Long): Long = try {
        Math.multiplyExact(seconds, 1_000L)
    } catch (_: ArithmeticException) {
        Long.MAX_VALUE
    }
}
