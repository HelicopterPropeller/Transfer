package com.example.transfer.service

import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking

internal object ServiceShutdownJobs {
    fun cancelAndJoin(jobs: Iterable<Job?>, currentJob: Job? = null) {
        val snapshots = jobs.filterNotNull().filterNot { it === currentJob }.distinct()
        snapshots.forEach { it.cancel() }
        runBlocking {
            snapshots.forEach { job -> runCatching { job.cancelAndJoin() } }
        }
    }
}
