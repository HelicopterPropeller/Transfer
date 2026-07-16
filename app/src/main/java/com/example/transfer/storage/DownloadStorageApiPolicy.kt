package com.example.transfer.storage

import java.io.IOException

internal enum class MediaStoreLocationOperation {
    REOPEN,
    OPEN_INPUT,
    PUBLISH,
    RECOVER_PUBLISHED,
    DELETE
}

internal enum class MediaStoreLocationDecision {
    USE_MEDIA_STORE,
    RETURN_NULL,
    THROW_UNSUPPORTED,
    IGNORE
}

internal object DownloadStorageApiPolicy {
    private const val MEDIA_STORE_DOWNLOADS_SDK = 29

    fun decide(
        sdkInt: Int,
        operation: MediaStoreLocationOperation
    ): MediaStoreLocationDecision {
        if (sdkInt >= MEDIA_STORE_DOWNLOADS_SDK) {
            return MediaStoreLocationDecision.USE_MEDIA_STORE
        }
        return when (operation) {
            MediaStoreLocationOperation.REOPEN,
            MediaStoreLocationOperation.OPEN_INPUT,
            MediaStoreLocationOperation.RECOVER_PUBLISHED ->
                MediaStoreLocationDecision.RETURN_NULL

            MediaStoreLocationOperation.PUBLISH ->
                MediaStoreLocationDecision.THROW_UNSUPPORTED

            MediaStoreLocationOperation.DELETE ->
                MediaStoreLocationDecision.IGNORE
        }
    }
}

internal enum class MediaStoreDeleteState {
    ABSENT,
    UNKNOWN,
    OWNED_PENDING,
    OWNED_PUBLISHED,
    UNOWNED
}

internal interface MediaStoreDeleteGateway {
    fun delete(): Int
    fun queryState(): MediaStoreDeleteState
}

internal object MediaStoreDeleteVerifier {
    fun deleteOwnedPending(gateway: MediaStoreDeleteGateway) {
        if (gateway.delete() > 0) return
        when (gateway.queryState()) {
            MediaStoreDeleteState.ABSENT -> Unit
            MediaStoreDeleteState.UNKNOWN ->
                throw IOException("Unable to confirm MediaStore download deletion")
            MediaStoreDeleteState.OWNED_PENDING ->
                throw IOException("MediaStore download still exists after deletion")
            MediaStoreDeleteState.OWNED_PUBLISHED ->
                throw SecurityException("MediaStore download was published during deletion")
            MediaStoreDeleteState.UNOWNED ->
                throw SecurityException("MediaStore download is no longer owned by this app")
        }
    }
}
