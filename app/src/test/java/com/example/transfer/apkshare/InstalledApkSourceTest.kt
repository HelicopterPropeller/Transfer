package com.example.transfer.apkshare

import java.io.File
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class InstalledApkSourceTest {
    @get:Rule
    val temporary = TemporaryFolder()

    @Test
    fun `split install is rejected without copying base apk`() {
        val base = temporary.newFile("base.apk").apply { writeBytes(byteArrayOf(1, 2, 3)) }
        val cache = temporary.newFolder("cache")

        val result = InstalledApkSource().prepare(
            InstalledApkDescriptor(base, listOf(File("config.en.apk")), "1.0", 1),
            cache,
        )

        assertTrue(result is ApkPreparationResult.SplitInstallUnsupported)
        assertTrue(cache.listFiles().orEmpty().isEmpty())
    }

    @Test
    fun `monolithic apk is copied and hashed`() {
        val base = temporary.newFile("base.apk").apply { writeText("transfer-apk") }

        val result = InstalledApkSource().prepare(
            InstalledApkDescriptor(base, emptyList(), "1.0", 1),
            temporary.newFolder("cache"),
        ) as ApkPreparationResult.Ready

        assertEquals("Transfer-1.0.apk", result.artifact.fileName)
        assertEquals(base.length(), result.artifact.size)
        assertEquals(64, result.artifact.sha256.length)
        assertArrayEquals(base.readBytes(), result.artifact.file.readBytes())
    }
}
