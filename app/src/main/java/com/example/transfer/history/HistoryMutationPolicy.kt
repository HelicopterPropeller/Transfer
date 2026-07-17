package com.example.transfer.history

internal object HistoryMutationPolicy {
    fun mutate(
        cancelActiveOutgoing: Boolean,
        cancel: () -> Unit,
        mutation: () -> Unit,
    ) {
        if (cancelActiveOutgoing) cancel()
        mutation()
    }
}
