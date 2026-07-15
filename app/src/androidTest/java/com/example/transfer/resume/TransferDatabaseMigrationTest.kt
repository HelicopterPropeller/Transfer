package com.example.transfer.resume

import androidx.room.testing.MigrationTestHelper
import androidx.sqlite.db.SupportSQLiteDatabase
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.example.transfer.history.TransferHistoryDatabase
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class TransferDatabaseMigrationTest {
    @get:Rule
    val helper = MigrationTestHelper(
        InstrumentationRegistry.getInstrumentation(),
        TransferHistoryDatabase::class.java
    )

    @Test
    fun migration1To2PreservesHistoryAndCreatesResumeTables() {
        helper.createDatabase(TEST_DATABASE, 1).apply {
            createVersionOneHistoryTable()
            execSQL(
                """
                INSERT INTO transfer_history (
                    id, direction, fileName, fileSize, mimeType,
                    peerId, peerName, peerAddress, status, startedAt,
                    finishedAt, errorMessage, sourceUri, receivedUri
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                arrayOf(
                    7L, "SEND", "known.bin", 42L, "application/octet-stream",
                    "peer-1", "Known peer", "192.0.2.1", "SUCCESS", 1000L,
                    2000L, null, "content://source/7", null
                )
            )
            close()
        }

        val migrated = helper.runMigrationsAndValidate(
            TEST_DATABASE,
            2,
            true,
            TransferHistoryDatabase.MIGRATION_1_2
        )

        migrated.query("SELECT * FROM transfer_history").use { cursor ->
            assertTrue(cursor.moveToFirst())
            assertEquals(7L, cursor.getLong(cursor.getColumnIndexOrThrow("id")))
            assertEquals("SEND", cursor.getString(cursor.getColumnIndexOrThrow("direction")))
            assertEquals("known.bin", cursor.getString(cursor.getColumnIndexOrThrow("fileName")))
            assertEquals(42L, cursor.getLong(cursor.getColumnIndexOrThrow("fileSize")))
            assertEquals(
                "application/octet-stream",
                cursor.getString(cursor.getColumnIndexOrThrow("mimeType"))
            )
            assertEquals("peer-1", cursor.getString(cursor.getColumnIndexOrThrow("peerId")))
            assertEquals("Known peer", cursor.getString(cursor.getColumnIndexOrThrow("peerName")))
            assertEquals("192.0.2.1", cursor.getString(cursor.getColumnIndexOrThrow("peerAddress")))
            assertEquals("SUCCESS", cursor.getString(cursor.getColumnIndexOrThrow("status")))
            assertEquals(1000L, cursor.getLong(cursor.getColumnIndexOrThrow("startedAt")))
            assertEquals(2000L, cursor.getLong(cursor.getColumnIndexOrThrow("finishedAt")))
            assertTrue(cursor.isNull(cursor.getColumnIndexOrThrow("errorMessage")))
            assertEquals(
                "content://source/7",
                cursor.getString(cursor.getColumnIndexOrThrow("sourceUri"))
            )
            assertTrue(cursor.isNull(cursor.getColumnIndexOrThrow("receivedUri")))
            assertTrue(!cursor.moveToNext())
        }
        assertTableEmpty(migrated, "incoming_checkpoints")
        assertTableEmpty(migrated, "outgoing_resume_links")
        migrated.close()
    }

    private fun SupportSQLiteDatabase.createVersionOneHistoryTable() {
        execSQL(
            """
            CREATE TABLE IF NOT EXISTS transfer_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                direction TEXT NOT NULL,
                fileName TEXT NOT NULL,
                fileSize INTEGER NOT NULL,
                mimeType TEXT NOT NULL,
                peerId TEXT,
                peerName TEXT,
                peerAddress TEXT,
                status TEXT NOT NULL,
                startedAt INTEGER NOT NULL,
                finishedAt INTEGER,
                errorMessage TEXT,
                sourceUri TEXT,
                receivedUri TEXT
            )
            """.trimIndent()
        )
    }

    private fun assertTableEmpty(database: SupportSQLiteDatabase, table: String) {
        database.query("SELECT COUNT(*) FROM $table").use { cursor ->
            assertTrue(cursor.moveToFirst())
            assertEquals(0, cursor.getInt(0))
        }
    }

    private companion object {
        const val TEST_DATABASE = "resume-migration-test"
    }
}
