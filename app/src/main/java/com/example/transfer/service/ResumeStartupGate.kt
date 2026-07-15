package com.example.transfer.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
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

    fun launchWhenReady(action: suspend () -> Unit): Job = scope.launch {
        awaitReady()
        action()
    }
}
