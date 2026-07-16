package com.example.transfer.service

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

internal suspend fun finalizeOutgoingNetworkResult(
    networkResult: Result<Unit>,
    cleanup: suspend () -> Unit
): Result<Unit> {
    if (networkResult.isSuccess) {
        withContext(NonCancellable) {
            runCatching { cleanup() }
        }
    }
    return networkResult
}
