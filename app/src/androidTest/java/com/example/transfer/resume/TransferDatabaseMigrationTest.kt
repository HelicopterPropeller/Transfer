package com.example.transfer.resume

import androidx.room.Room
import androidx.room.testing.MigrationTestHelper
import androidx.sqlite.db.SupportSQLiteDatabase
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.example.transfer.history.TransferHistoryDatabase
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
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
            3,
            true,
            TransferHistoryDatabase.MIGRATION_1_2,
            TransferHistoryDatabase.MIGRATION_2_3
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
        assertEquals(
            setOf("generation", "sessionToken", "sessionClaimedAt", "retiredStorageKind", "retiredStorageValue"),
            tableColumns(migrated, "incoming_checkpoints").intersect(
                setOf("generation", "sessionToken", "sessionClaimedAt", "retiredStorageKind", "retiredStorageValue")
            )
        )
        migrated.close()
    }

    @Test
    fun migration2To3PreservesCheckpointAndAddsOwnershipColumns() {
        helper.createDatabase(TEST_DATABASE, 2).apply {
            createVersionOneHistoryTable()
            TransferHistoryDatabase.MIGRATION_1_2.migrate(this)
            execSQL(
                """INSERT INTO incoming_checkpoints
                (transferId,senderDeviceId,fileName,displayName,mimeType,fileSize,chunkSize,
                 confirmedBytes,nextChunkIndex,chainDigest,lastChunkHash,storageKind,storageValue,
                 createdAt,updatedAt,expiresAt,cleanupToken,cleanupClaimedAt)
                VALUES ('kept','sender','a.bin','a.bin','application/octet-stream',1,1048576,
                 0,0,X'00',X'00','LEGACY_FILE','old.part',1,1,2,NULL,NULL)""".trimIndent()
            )
            close()
        }
        val migrated = helper.runMigrationsAndValidate(
            TEST_DATABASE, 3, true, TransferHistoryDatabase.MIGRATION_2_3
        )
        migrated.query(
            "SELECT transferId,generation,sessionToken,retiredStorageValue,operationState " +
                "FROM incoming_checkpoints"
        ).use {
            assertTrue(it.moveToFirst())
            assertEquals("kept", it.getString(0))
            assertEquals(0L, it.getLong(1))
            assertTrue(it.isNull(2))
            assertTrue(it.isNull(3))
            assertEquals(IncomingOperationState.IDLE, it.getString(4))
        }
        migrated.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='incoming_staging_journal'"
        ).use {
            assertTrue(it.moveToFirst())
        }
        migrated.close()
    }

    @Test
    fun staleExpectedChunkIndexDoesNotAdvanceRealRoomCheckpoint() = runBlocking {
        withResumeDatabase { database ->
            val dao = database.resumeDao()
            dao.upsertIncoming(incomingEntity("t1").copy(nextChunkIndex = 2, confirmedBytes = 16L))

            val updated = dao.commitIncomingChunk(
                transferId = "t1",
                expectedNextChunkIndex = 1,
                confirmedBytes = 24L,
                nextChunkIndex = 3,
                chainDigest = byteArrayOf(3),
                lastChunkHash = byteArrayOf(4),
                updatedAt = 20L,
                expiresAt = 200L
            )

            assertEquals(0, updated)
            assertEquals(16L, dao.findIncoming("t1")?.confirmedBytes)
            assertEquals(2, dao.findIncoming("t1")?.nextChunkIndex)
        }
    }

    @Test
    fun duplicateSourceUriAndPeerResolvesActualRealRoomOwner() = runBlocking {
        withResumeDatabase { database ->
            val dao = database.resumeDao()
            val first = dao.resolveOutgoing(outgoingEntity("t1"))
            val second = dao.resolveOutgoing(outgoingEntity("t2"))

            assertEquals("t1", first.transferId)
            assertEquals("t1", second.transferId)
            assertEquals("t1", dao.findOutgoing("content://a", "peer-1")?.transferId)
        }
    }

    @Test
    fun realRoomSessionOwnershipHasOneWinnerAndGuardsCommit() = runBlocking {
        withResumeDatabase { database ->
            val dao = database.resumeDao()
            val row = incomingEntity("t1")
            dao.upsertIncoming(row)
            val args = arrayOf(row.transferId, row.storageKind, row.storageValue)
            assertEquals(1, dao.acquireIncomingSession(
                args[0], args[1], args[2], row.generation, row.nextChunkIndex,
                row.confirmedBytes, row.chainDigest, row.lastChunkHash,
                "winner", 20L, 200L
            ))
            assertEquals(0, dao.acquireIncomingSession(
                args[0], args[1], args[2], row.generation, row.nextChunkIndex,
                row.confirmedBytes, row.chainDigest, row.lastChunkHash,
                "loser", 20L, 200L
            ))
            assertEquals(0, dao.commitOwnedIncomingChunk(
                row.transferId, "loser", row.generation, row.storageKind, row.storageValue,
                row.nextChunkIndex, 16L, 2, byteArrayOf(3), byteArrayOf(4), 30L, 300L
            ))
            assertEquals(1, dao.commitOwnedIncomingChunk(
                row.transferId, "winner", row.generation, row.storageKind, row.storageValue,
                row.nextChunkIndex, 16L, 2, byteArrayOf(3), byteArrayOf(4), 30L, 300L
            ))
        }
    }

    @Test
    fun atomicClaimReturnsOnlyRowsOwnedByItsToken() = runBlocking {
        withResumeDatabase { database ->
            val dao = database.resumeDao()
            dao.upsertIncoming(incomingEntity("expired-1").copy(expiresAt = 90L))
            dao.upsertIncoming(incomingEntity("expired-2").copy(expiresAt = 100L))
            dao.upsertIncoming(incomingEntity("fresh").copy(expiresAt = 101L))
            dao.upsertIncoming(
                incomingEntity("recent-claim").copy(
                    expiresAt = 90L,
                    cleanupToken = "other",
                    cleanupClaimedAt = 90L
                )
            )
            dao.upsertIncoming(
                incomingEntity("stale-claim").copy(
                    expiresAt = 90L,
                    cleanupToken = "old",
                    cleanupClaimedAt = 50L
                )
            )

            val claimed = dao.claimExpiredIncoming(100L, staleClaimBefore = 80L, token = "ours")
            val competing = dao.claimExpiredIncoming(100L, staleClaimBefore = 80L, token = "theirs")

            assertEquals(setOf("expired-1", "expired-2", "stale-claim"), claimed.map { it.transferId }.toSet())
            assertTrue(claimed.all { it.cleanupToken == "ours" && it.cleanupClaimedAt == 100L })
            assertTrue(competing.isEmpty())
            assertEquals("other", dao.findIncoming("recent-claim")?.cleanupToken)
            assertNull(dao.findIncoming("fresh")?.cleanupToken)
        }
    }

    @Test
    fun claimedRealRoomCheckpointRejectsProgressAndExpiryUpdates() = runBlocking {
        withResumeDatabase { database ->
            val dao = database.resumeDao()
            dao.upsertIncoming(incomingEntity("t1").copy(expiresAt = 40L))
            dao.claimExpiredIncoming(50L, staleClaimBefore = 0L, token = "claim-1")

            assertEquals(
                0,
                dao.commitIncomingChunk(
                    transferId = "t1",
                    expectedNextChunkIndex = 1,
                    confirmedBytes = 16L,
                    nextChunkIndex = 2,
                    chainDigest = byteArrayOf(3),
                    lastChunkHash = byteArrayOf(4),
                    updatedAt = 60L,
                    expiresAt = 200L
                )
            )
            assertEquals(0, dao.updateIncomingExpiry("t1", updatedAt = 60L, expiresAt = 200L))
            assertEquals(8L, dao.findIncoming("t1")?.confirmedBytes)
            assertEquals(40L, dao.findIncoming("t1")?.expiresAt)
        }
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

    private fun tableColumns(database: SupportSQLiteDatabase, table: String): Set<String> =
        database.query("PRAGMA table_info(`$table`)").use { cursor ->
            buildSet {
                val name = cursor.getColumnIndexOrThrow("name")
                while (cursor.moveToNext()) add(cursor.getString(name))
            }
        }

    private suspend fun withResumeDatabase(block: suspend (TransferHistoryDatabase) -> Unit) {
        val database = Room.inMemoryDatabaseBuilder(
            InstrumentationRegistry.getInstrumentation().targetContext,
            TransferHistoryDatabase::class.java
        ).build()
        try {
            block(database)
        } finally {
            database.close()
        }
    }

    private fun incomingEntity(transferId: String) = IncomingCheckpointEntity(
        transferId = transferId,
        senderDeviceId = "sender-1",
        fileName = "a.bin",
        displayName = "a.bin",
        mimeType = "application/octet-stream",
        fileSize = 32L,
        chunkSize = 8,
        confirmedBytes = 8L,
        nextChunkIndex = 1,
        chainDigest = byteArrayOf(1),
        lastChunkHash = byteArrayOf(2),
        storageKind = ResumeStorageKind.MEDIA_STORE,
        storageValue = "content://pending/t1",
        createdAt = 10L,
        updatedAt = 10L,
        expiresAt = 100L,
        cleanupToken = null,
        cleanupClaimedAt = null
    )

    private fun outgoingEntity(transferId: String) = OutgoingResumeLinkEntity(
        transferId = transferId,
        sourceUri = "content://a",
        peerDeviceId = "peer-1",
        fileName = "a.bin",
        mimeType = "application/octet-stream",
        fileSize = 8L,
        lastModified = 10L,
        chunkSize = 1,
        createdAt = 1L,
        updatedAt = 1L
    )

    private companion object {
        const val TEST_DATABASE = "resume-migration-test"
    }
}
