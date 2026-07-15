package com.example.transfer.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.launch

internal class ResumeStartupGate(
    private val scope: CoroutineScope,
    recover: suspend () -> Unit,
    cleanup: suspend () -> Unit
) {
    private val initialization: Deferred<Unit> = scope.async {
        recover()
        cleanup()
    }

    suspend fun awaitReady() {
        initialization.await()
    }

    fun launchWhenReady(
        onFailure: (Throwable) -> Unit = {},
        action: suspend () -> Unit
    ): Job = scope.launch {
        try {
            awaitReady()
            action()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            runCatching { onFailure(error) }
        }
    }
}
