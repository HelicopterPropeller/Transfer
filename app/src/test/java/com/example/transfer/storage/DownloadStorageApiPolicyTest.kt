package com.example.transfer.storage

import java.io.IOException
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class DownloadStorageApiPolicyTest {
    @Test
    fun `zero row delete is idempotent only when requery confirms absence`() {
        val resolver = FakeMediaStoreDeleteResolver(
            deletedRows = 0,
            state = MediaStoreDeleteState.ABSENT
        )

        MediaStoreDeleteVerifier.deleteOwnedPending(resolver)

        assertEquals(1, resolver.deleteCalls)
        assertEquals(1, resolver.queryCalls)
    }

    @Test
    fun `zero row delete throws when owned pending row still exists`() {
        val resolver = FakeMediaStoreDeleteResolver(
            deletedRows = 0,
            state = MediaStoreDeleteState.OWNED_PENDING
        )

        assertThrows(IOException::class.java) {
            MediaStoreDeleteVerifier.deleteOwnedPending(resolver)
        }
        assertEquals(1, resolver.queryCalls)
    }

    @Test
    fun `zero row delete never treats published or unowned row as success`() {
        listOf(
            MediaStoreDeleteState.OWNED_PUBLISHED,
            MediaStoreDeleteState.UNOWNED
        ).forEach { state ->
            val resolver = FakeMediaStoreDeleteResolver(0, state)

            assertThrows(SecurityException::class.java) {
                MediaStoreDeleteVerifier.deleteOwnedPending(resolver)
            }
            assertEquals(1, resolver.queryCalls)
        }
    }

    @Test
    fun `zero row delete does not treat an unavailable requery as confirmed absence`() {
        val resolver = FakeMediaStoreDeleteResolver(
            deletedRows = 0,
            state = MediaStoreDeleteState.UNKNOWN
        )

        assertThrows(IOException::class.java) {
            MediaStoreDeleteVerifier.deleteOwnedPending(resolver)
        }
    }

    @Test
    fun `positive delete result does not need a second query`() {
        val resolver = FakeMediaStoreDeleteResolver(
            deletedRows = 1,
            state = MediaStoreDeleteState.OWNED_PENDING
        )

        MediaStoreDeleteVerifier.deleteOwnedPending(resolver)

        assertEquals(0, resolver.queryCalls)
    }

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

private class FakeMediaStoreDeleteResolver(
    private val deletedRows: Int,
    private val state: MediaStoreDeleteState
) : MediaStoreDeleteGateway {
    var deleteCalls = 0
    var queryCalls = 0

    override fun delete(): Int {
        deleteCalls++
        return deletedRows
    }

    override fun queryState(): MediaStoreDeleteState {
        queryCalls++
        return state
    }
}
