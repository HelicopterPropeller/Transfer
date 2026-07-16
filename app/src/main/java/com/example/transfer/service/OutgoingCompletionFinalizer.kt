package com.example.transfer.service

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

internal suspend fun finalizeOutgoingNetworkResult(
    networkResult: Result<Unit>,
    cleanupAttempts: Int = 3,
    onCleanupFailure: (Throwable) -> Unit = {},
    cleanup: suspend () -> Unit
): Result<Unit> {
    require(cleanupAttempts > 0)
    if (networkResult.isSuccess) {
        withContext(NonCancellable) {
            var lastFailure: Exception? = null
            repeat(cleanupAttempts) {
                try {
                    cleanup()
                    return@withContext
                } catch (error: Exception) {
                    lastFailure = error
                }
            }
            onCleanupFailure(requireNotNull(lastFailure))
        }
    }
    return networkResult
}
