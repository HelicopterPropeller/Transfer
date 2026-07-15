package com.example.transfer.storage

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
