package com.example.transfer.ui

import org.junit.Assert.assertEquals
import org.junit.Test

class SelectedFileMetadataReaderTest {
    @Test
    fun `document provider rejecting document timestamp falls back to media seconds`() {
        val provider = FakeMetadataProvider(
            base = SelectedFileBaseMetadata("a.bin", 7L),
            acceptedTimestamps = mapOf(
                SelectedFileTimestampColumn.MEDIA_SECONDS to 123L
            )
        )

        val metadata = SelectedFileMetadataReader.read(
            SelectedFileProviderKind.DOCUMENT,
            provider::queryBase,
            provider::queryTimestamp
        )

        assertEquals(123_000L, metadata?.lastModified)
        assertEquals(
            listOf(
                SelectedFileTimestampColumn.DOCUMENT_MILLIS,
                SelectedFileTimestampColumn.MEDIA_SECONDS
            ),
            provider.timestampQueries
        )
    }

    @Test
    fun `media provider rejecting media timestamp falls back to document millis`() {
        val provider = FakeMetadataProvider(
            base = SelectedFileBaseMetadata("b.bin", 8L),
            acceptedTimestamps = mapOf(
                SelectedFileTimestampColumn.DOCUMENT_MILLIS to 456L
            )
        )

        val metadata = SelectedFileMetadataReader.read(
            SelectedFileProviderKind.MEDIA,
            provider::queryBase,
            provider::queryTimestamp
        )

        assertEquals(456L, metadata?.lastModified)
        assertEquals(
            listOf(
                SelectedFileTimestampColumn.MEDIA_SECONDS,
                SelectedFileTimestampColumn.DOCUMENT_MILLIS
            ),
            provider.timestampQueries
        )
    }

    @Test
    fun `provider rejecting both timestamp columns retains base metadata`() {
        val provider = FakeMetadataProvider(
            base = SelectedFileBaseMetadata("c.bin", 9L),
            acceptedTimestamps = emptyMap()
        )

        val metadata = SelectedFileMetadataReader.read(
            SelectedFileProviderKind.UNKNOWN,
            provider::queryBase,
            provider::queryTimestamp
        )

        assertEquals("c.bin", metadata?.displayName)
        assertEquals(9L, metadata?.size)
        assertEquals(null, metadata?.lastModified)
    }
}

private class FakeMetadataProvider(
    private val base: SelectedFileBaseMetadata,
    private val acceptedTimestamps: Map<SelectedFileTimestampColumn, Long>
) {
    val timestampQueries = mutableListOf<SelectedFileTimestampColumn>()

    fun queryBase(): SelectedFileBaseMetadata = base

    fun queryTimestamp(column: SelectedFileTimestampColumn): Long? {
        timestampQueries += column
        if (column !in acceptedTimestamps) {
            throw IllegalArgumentException("unsupported projection: $column")
        }
        return acceptedTimestamps[column]
    }
}
