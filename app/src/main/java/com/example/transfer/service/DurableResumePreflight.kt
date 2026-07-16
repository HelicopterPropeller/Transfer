package com.example.transfer.service

import java.util.concurrent.CancellationException

internal data class DurablePreflightReady<T, P, R>(
    val item: T,
    val prepared: P,
    val remote: R
)

internal data class DurablePreparationFailure<T>(
    val item: T,
    val error: Exception
)

internal data class DurableResumePreflightResult<T, P, R>(
    val ready: List<DurablePreflightReady<T, P, R>>,
    val preparationFailures: List<DurablePreparationFailure<T>>
)

internal suspend fun <T, P, R> durableResumePreflight(
    items: List<T>,
    prepare: suspend (T) -> P,
    persistPrepared: suspend (T, P) -> Unit,
    query: suspend (P) -> R
): DurableResumePreflightResult<T, P, R> {
    val locallyPrepared = ArrayList<Pair<T, P>>(items.size)
    val failures = mutableListOf<DurablePreparationFailure<T>>()
    items.forEach { item ->
        try {
            val prepared = prepare(item)
            persistPrepared(item, prepared)
            locallyPrepared += item to prepared
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            failures += DurablePreparationFailure(item, error)
        }
    }

    val ready = locallyPrepared.map { (item, prepared) ->
        DurablePreflightReady(item, prepared, query(prepared))
    }
    return DurableResumePreflightResult(ready, failures)
}
