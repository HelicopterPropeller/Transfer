package com.example.transfer.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import java.util.concurrent.CancellationException

internal class HistoryStartupGate(
    scope: CoroutineScope,
    initialize: suspend () -> Unit
) {
    private val initialization: Deferred<Unit> = scope.async {
        try {
            initialize()
        } catch (exception: CancellationException) {
            throw exception
        } catch (_: Exception) {
            // History cleanup is best-effort and must not permanently block new sends.
        }
    }

    suspend fun awaitReady() {
        initialization.await()
    }
}
