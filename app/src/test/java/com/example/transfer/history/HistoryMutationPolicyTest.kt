package com.example.transfer.history

import org.junit.Assert.assertEquals
import org.junit.Test

class HistoryMutationPolicyTest {
    @Test
    fun `active outgoing cancellation is requested before deleting history`() {
        val events = mutableListOf<String>()

        HistoryMutationPolicy.mutate(
            cancelActiveOutgoing = true,
            cancel = { events += "cancel" },
            mutation = { events += "delete" }
        )

        assertEquals(listOf("cancel", "delete"), events)
    }

    @Test
    fun `terminal rows are deleted without cancelling a transfer`() {
        val events = mutableListOf<String>()

        HistoryMutationPolicy.mutate(
            cancelActiveOutgoing = false,
            cancel = { events += "cancel" },
            mutation = { events += "delete" }
        )

        assertEquals(listOf("delete"), events)
    }
}
