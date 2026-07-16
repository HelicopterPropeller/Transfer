package com.example.transfer.history

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.migration.Migration
import androidx.sqlite.db.SupportSQLiteDatabase
import com.example.transfer.batch.OutgoingBatchDao
import com.example.transfer.batch.OutgoingBatchEntity
import com.example.transfer.batch.OutgoingBatchItemEntity
import com.example.transfer.resume.IncomingCheckpointEntity
import com.example.transfer.resume.IncomingStagingJournalEntity
import com.example.transfer.resume.OutgoingResumeLinkEntity
import com.example.transfer.resume.CompletedReceiptEntity
import com.example.transfer.resume.ResumeDao

@Database(
    entities = [
        TransferHistoryEntity::class,
        IncomingCheckpointEntity::class,
        IncomingStagingJournalEntity::class,
        OutgoingResumeLinkEntity::class,
        CompletedReceiptEntity::class,
        OutgoingBatchEntity::class,
        OutgoingBatchItemEntity::class
    ],
    version = 5,
    exportSchema = false
)
abstract class TransferHistoryDatabase : RoomDatabase() {
    abstract fun transferHistoryDao(): TransferHistoryDao
    abstract fun resumeDao(): ResumeDao
    abstract fun outgoingBatchDao(): OutgoingBatchDao

    companion object {
        val MIGRATION_1_2 = object : Migration(1, 2) {
            override fun migrate(database: SupportSQLiteDatabase) {
                database.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS incoming_checkpoints (
                        transferId TEXT NOT NULL PRIMARY KEY,
                        senderDeviceId TEXT NOT NULL,
                        fileName TEXT NOT NULL,
                        displayName TEXT NOT NULL,
                        mimeType TEXT NOT NULL,
                        fileSize INTEGER NOT NULL,
                        chunkSize INTEGER NOT NULL,
                        confirmedBytes INTEGER NOT NULL,
                        nextChunkIndex INTEGER NOT NULL,
                        chainDigest BLOB NOT NULL,
                        lastChunkHash BLOB NOT NULL,
                        storageKind TEXT NOT NULL,
                        storageValue TEXT NOT NULL,
                        createdAt INTEGER NOT NULL,
                        updatedAt INTEGER NOT NULL,
                        expiresAt INTEGER NOT NULL,
                        cleanupToken TEXT,
                        cleanupClaimedAt INTEGER
                    )
                    """.trimIndent()
                )
                database.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS outgoing_resume_links (
                        transferId TEXT NOT NULL PRIMARY KEY,
                        sourceUri TEXT NOT NULL,
                        peerDeviceId TEXT NOT NULL,
                        fileName TEXT NOT NULL,
                        mimeType TEXT NOT NULL,
                        fileSize INTEGER NOT NULL,
                        lastModified INTEGER,
                        chunkSize INTEGER NOT NULL,
                        createdAt INTEGER NOT NULL,
                        updatedAt INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
                database.execSQL(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS
                    index_outgoing_resume_links_sourceUri_peerDeviceId
                    ON outgoing_resume_links (sourceUri, peerDeviceId)
                    """.trimIndent()
                )
            }
        }

        val MIGRATION_2_3 = object : Migration(2, 3) {
            override fun migrate(database: SupportSQLiteDatabase) {
                database.execSQL("ALTER TABLE incoming_checkpoints ADD COLUMN generation INTEGER NOT NULL DEFAULT 0")
                database.execSQL("ALTER TABLE incoming_checkpoints ADD COLUMN sessionToken TEXT DEFAULT NULL")
                database.execSQL("ALTER TABLE incoming_checkpoints ADD COLUMN sessionClaimedAt INTEGER DEFAULT NULL")
                database.execSQL("ALTER TABLE incoming_checkpoints ADD COLUMN retiredStorageKind TEXT DEFAULT NULL")
                database.execSQL("ALTER TABLE incoming_checkpoints ADD COLUMN retiredStorageValue TEXT DEFAULT NULL")
                database.execSQL("ALTER TABLE incoming_checkpoints ADD COLUMN operationState TEXT NOT NULL DEFAULT 'IDLE'")
                database.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS incoming_staging_journal (
                        transferId TEXT NOT NULL PRIMARY KEY,
                        stagingId TEXT NOT NULL,
                        createdAt INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
            }
        }

        val MIGRATION_3_4 = object : Migration(3, 4) {
            override fun migrate(database: SupportSQLiteDatabase) {
                database.execSQL(
                    "ALTER TABLE incoming_checkpoints ADD COLUMN completingFinalDigest BLOB DEFAULT NULL"
                )
                database.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS completed_receipts (
                        transferId TEXT NOT NULL PRIMARY KEY,
                        senderDeviceId TEXT NOT NULL,
                        fileName TEXT NOT NULL,
                        mimeType TEXT NOT NULL,
                        fileSize INTEGER NOT NULL,
                        chunkSize INTEGER NOT NULL,
                        finalDigest BLOB NOT NULL,
                        publishedUri TEXT,
                        publishedName TEXT NOT NULL,
                        completedAt INTEGER NOT NULL,
                        expiresAt INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
            }
        }

        val MIGRATION_4_5 = object : Migration(4, 5) {
            override fun migrate(database: SupportSQLiteDatabase) {
                database.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS outgoing_batches (
                        batchId TEXT NOT NULL PRIMARY KEY,
                        peerDeviceId TEXT NOT NULL,
                        state TEXT NOT NULL,
                        createdAt INTEGER NOT NULL,
                        updatedAt INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
                database.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS outgoing_batch_items (
                        batchId TEXT NOT NULL,
                        position INTEGER NOT NULL,
                        sourceUri TEXT NOT NULL,
                        displayName TEXT NOT NULL,
                        mimeType TEXT NOT NULL,
                        fileSize INTEGER NOT NULL,
                        lastModified INTEGER,
                        transferId TEXT,
                        state TEXT NOT NULL,
                        updatedAt INTEGER NOT NULL,
                        PRIMARY KEY(batchId, position),
                        FOREIGN KEY(batchId) REFERENCES outgoing_batches(batchId)
                            ON UPDATE NO ACTION ON DELETE CASCADE
                    )
                    """.trimIndent()
                )
                database.execSQL(
                    "CREATE INDEX IF NOT EXISTS index_outgoing_batch_items_batchId " +
                        "ON outgoing_batch_items(batchId)"
                )
            }
        }

        @Volatile
        private var instance: TransferHistoryDatabase? = null

        fun getInstance(context: Context): TransferHistoryDatabase =
            instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext,
                    TransferHistoryDatabase::class.java,
                    "transfer_history.db"
                ).addMigrations(MIGRATION_1_2, MIGRATION_2_3, MIGRATION_3_4, MIGRATION_4_5)
                    .build()
                    .also { instance = it }
            }
    }
}
