package com.example.transfer.service

import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class DurableResumePreflightTest {
    @Test
    fun `all local transfer ids are saved before first remote query`() = runBlocking {
        val events = mutableListOf<String>()

        val result = runCatching {
            durableResumePreflight(
                items = listOf("a", "b", "c"),
                prepare = { item -> events += "prepare:$item"; "transfer-$item" },
                persistPrepared = { item, _ -> events += "persist:$item" },
                query = { prepared ->
                    events += "query:$prepared"
                    if (prepared == "transfer-b") error("offline") else prepared
                }
            )
        }

        assertTrue(result.isFailure)
        assertEquals(
            listOf(
                "prepare:a", "persist:a",
                "prepare:b", "persist:b",
                "prepare:c", "persist:c"
            ),
            events.take(6)
        )
    }

    @Test
    fun `one unreadable source does not prevent later local preparation`() = runBlocking {
        val events = mutableListOf<String>()

        val result = durableResumePreflight(
            items = listOf("a", "bad", "c"),
            prepare = { item ->
                events += "prepare:$item"
                if (item == "bad") error("unreadable")
                "transfer-$item"
            },
            persistPrepared = { item, _ -> events += "persist:$item" },
            query = { prepared -> events += "query:$prepared"; prepared }
        )

        assertEquals(listOf("bad"), result.preparationFailures.map { it.item })
        assertEquals(listOf("a", "c"), result.ready.map { it.item })
        assertTrue(events.indexOf("persist:c") < events.indexOf("query:transfer-a"))
    }
}
