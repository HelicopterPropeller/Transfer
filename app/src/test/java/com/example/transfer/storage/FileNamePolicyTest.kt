package com.example.transfer.storage

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test

class FileNamePolicyTest {
    @Test
    fun `sanitize removes traversal separators and controls`() {
        val result = FileNamePolicy.sanitize("../bad\\name\u0000.txt")
        assertEquals("bad_name.txt", result)
        assertFalse(result.contains(".."))
    }

    @Test
    fun `sanitize supplies fallback and limits length`() {
        assertEquals("received_file", FileNamePolicy.sanitize(" .. \u0000 "))
        assertEquals(180, FileNamePolicy.sanitize("a".repeat(250) + ".txt").length)
    }

    @Test
    fun `timestamp suffix preserves extension`() {
        assertEquals(
            "report_20260713_103000.pdf",
            FileNamePolicy.withTimestamp("report.pdf", "20260713_103000", 1)
        )
        assertEquals(
            "report_20260713_103000_2.pdf",
            FileNamePolicy.withTimestamp("report.pdf", "20260713_103000", 2)
        )
    }
}
