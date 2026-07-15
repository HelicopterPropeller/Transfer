package com.example.transfer.storage

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class LegacyDownloadFilesTest {
    @Test
    fun `published name is deterministic per partial and keeps extension`() {
        val first = LegacyDownloadFiles.stablePublishedName("report.pdf", "C:/Download/.transfer-a.part")
        val reopened = LegacyDownloadFiles.stablePublishedName("report.pdf", "C:/Download/.transfer-a.part")
        val second = LegacyDownloadFiles.stablePublishedName("report.pdf", "C:/Download/.transfer-b.part")

        assertEquals(first, reopened)
        assertNotEquals(first, second)
        assertTrue(first.startsWith("report_"))
        assertTrue(first.endsWith(".pdf"))
    }
    @get:Rule
    val temporaryFolder = TemporaryFolder()

    @Test
    fun `concurrent reservations never claim the same destination`() {
        val directory = temporaryFolder.newFolder("Transfer").canonicalFile
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(2)
        try {
            val reservations = List(2) {
                executor.submit(Callable {
                    start.await()
                    LegacyDownloadFiles.reserveFinalFile(directory, "report.pdf") {
                        "20260715_120000"
                    }
                })
            }

            start.countDown()
            val first = reservations[0].get()
            val second = reservations[1].get()

            assertNotEquals(first.canonicalPath, second.canonicalPath)
            assertEquals(setOf("report.pdf", "report_20260715_120000.pdf"), setOf(first.name, second.name))
            assertTrue(first.isFile)
            assertTrue(second.isFile)
            assertEquals(0, first.length())
            assertEquals(0, second.length())
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `reservation advances attempts when requested and timestamp names exist`() {
        val directory = temporaryFolder.newFolder("Transfer").canonicalFile
        File(directory, "report.pdf").createNewFile()
        File(directory, "report_20260715_120000.pdf").createNewFile()

        val reserved = LegacyDownloadFiles.reserveFinalFile(directory, "report.pdf") {
            "20260715_120000"
        }

        assertEquals("report_20260715_120000_2.pdf", reserved.name)
        assertTrue(reserved.isFile)
    }

    @Test
    fun `canonical containment rejects traversal outside transfer directory`() {
        val directory = temporaryFolder.newFolder("Transfer").canonicalFile
        val traversingPath = File(directory, "../outside.part").path

        assertThrows(SecurityException::class.java) {
            LegacyDownloadFiles.requireContained(directory, traversingPath)
        }
    }
}
