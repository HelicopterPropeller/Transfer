package com.example.transfer.storage

import org.junit.Assert.assertEquals
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File
import java.nio.file.Files
import java.nio.file.FileAlreadyExistsException
import java.io.IOException
import java.security.MessageDigest
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

    @Test
    fun `publish never overwrites a preexisting destination`() {
        val directory = temporaryFolder.newFolder("publish-preexisting").canonicalFile
        val partial = File(directory, ".transfer-a.part").apply { writeBytes(byteArrayOf(1, 2, 3)) }
        val digest = sha256(partial.readBytes())
        val preexisting = File(
            directory,
            LegacyDownloadFiles.stablePublishedName("report.pdf", partial.canonicalPath)
        ).apply { writeBytes(byteArrayOf(9, 9, 9)) }

        val published = LegacyDownloadFiles.publishWithoutOverwrite(
            partial, "report.pdf", digest, moveIntoPlace = ::moveExclusive
        )

        assertArrayEquals(byteArrayOf(9, 9, 9), preexisting.readBytes())
        assertNotEquals(preexisting.canonicalPath, published.canonicalPath)
        assertArrayEquals(byteArrayOf(1, 2, 3), published.readBytes())
        assertTrue(!partial.exists())
    }

    @Test
    fun `recovery finds an atomic move completed before receipt persistence`() {
        val directory = temporaryFolder.newFolder("publish-recover").canonicalFile
        val bytes = byteArrayOf(4, 5, 6)
        val partial = File(directory, ".transfer-b.part").apply { writeBytes(bytes) }
        val digest = sha256(bytes)
        var published: File? = null
        assertThrows(IOException::class.java) {
            LegacyDownloadFiles.publishWithoutOverwrite(
                partial, "video.bin", digest
            ) { source, target ->
                Files.move(source.toPath(), target.toPath())
                published = target
                throw IOException("process stopped after atomic move")
            }
        }

        val recovered = LegacyDownloadFiles.recoverPublished(
            partial, "video.bin", digest
        )

        assertEquals(published?.canonicalPath, recovered?.canonicalPath)
        assertArrayEquals(bytes, requireNotNull(published).readBytes())
        assertTrue(!partial.exists())
    }

    @Test
    fun `failed move leaves only the hidden partial and no visible incomplete file`() {
        val directory = temporaryFolder.newFolder("publish-move-failure").canonicalFile
        val bytes = byteArrayOf(7, 8, 9, 10)
        val partial = File(directory, ".transfer-c.part").apply { writeBytes(bytes) }
        val digest = sha256(bytes)
        var target: File? = null

        assertThrows(IOException::class.java) {
            LegacyDownloadFiles.publishWithoutOverwrite(
                partial, "archive.zip", digest
            ) { _, candidate ->
                target = candidate
                throw IOException("move failed before rename")
            }
        }

        assertArrayEquals(bytes, partial.readBytes())
        assertTrue(requireNotNull(target).exists().not())
    }

    @Test
    fun `concurrent destination creation is preserved and publish retries`() {
        val directory = temporaryFolder.newFolder("publish-concurrent").canonicalFile
        val bytes = byteArrayOf(11, 12, 13)
        val partial = File(directory, ".transfer-d.part").apply { writeBytes(bytes) }
        val digest = sha256(bytes)
        var racedTarget: File? = null
        val nonces = ArrayDeque(listOf("a".repeat(32), "b".repeat(32)))
        val published = LegacyDownloadFiles.publishWithoutOverwrite(
            partial,
            "notes.txt",
            digest,
            moveIntoPlace = { source, target ->
                if (racedTarget == null) {
                    target.writeBytes(byteArrayOf(99))
                    racedTarget = target
                    false
                } else {
                    moveExclusive(source, target)
                }
            },
            nonceFactory = { nonces.removeFirst() }
        )

        assertArrayEquals(byteArrayOf(99), requireNotNull(racedTarget).readBytes())
        assertNotEquals(racedTarget?.canonicalPath, published.canonicalPath)
        assertTrue(requireNotNull(racedTarget).name.contains("a".repeat(32)))
        assertTrue(published.name.contains("b".repeat(32)))
        assertArrayEquals(bytes, published.readBytes())
    }

    private fun sha256(bytes: ByteArray): ByteArray =
        MessageDigest.getInstance("SHA-256").digest(bytes)

    private fun moveExclusive(source: File, target: File): Boolean = try {
        Files.move(source.toPath(), target.toPath())
        true
    } catch (_: FileAlreadyExistsException) {
        false
    }
}
