package com.example.transfer.storage

import org.junit.Assert.assertEquals
import org.junit.Test

class DownloadStorageApiPolicyTest {
    @Test
    fun `api 28 rejects MediaStore locations according to each public operation contract`() {
        assertEquals(
            MediaStoreLocationDecision.RETURN_NULL,
            DownloadStorageApiPolicy.decide(28, MediaStoreLocationOperation.REOPEN)
        )
        assertEquals(
            MediaStoreLocationDecision.RETURN_NULL,
            DownloadStorageApiPolicy.decide(28, MediaStoreLocationOperation.OPEN_INPUT)
        )
        assertEquals(
            MediaStoreLocationDecision.THROW_UNSUPPORTED,
            DownloadStorageApiPolicy.decide(28, MediaStoreLocationOperation.PUBLISH)
        )
        assertEquals(
            MediaStoreLocationDecision.RETURN_NULL,
            DownloadStorageApiPolicy.decide(28, MediaStoreLocationOperation.RECOVER_PUBLISHED)
        )
        assertEquals(
            MediaStoreLocationDecision.IGNORE,
            DownloadStorageApiPolicy.decide(28, MediaStoreLocationOperation.DELETE)
        )
    }

    @Test
    fun `api 29 uses MediaStore for every public location operation`() {
        MediaStoreLocationOperation.entries.forEach { operation ->
            assertEquals(
                MediaStoreLocationDecision.USE_MEDIA_STORE,
                DownloadStorageApiPolicy.decide(29, operation)
            )
        }
    }
}
