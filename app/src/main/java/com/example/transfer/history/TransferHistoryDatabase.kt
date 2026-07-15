package com.example.transfer.history

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.migration.Migration
import androidx.sqlite.db.SupportSQLiteDatabase
import com.example.transfer.resume.IncomingCheckpointEntity
import com.example.transfer.resume.OutgoingResumeLinkEntity
import com.example.transfer.resume.ResumeDao

@Database(
    entities = [
        TransferHistoryEntity::class,
        IncomingCheckpointEntity::class,
        OutgoingResumeLinkEntity::class
    ],
    version = 2,
    exportSchema = false
)
abstract class TransferHistoryDatabase : RoomDatabase() {
    abstract fun transferHistoryDao(): TransferHistoryDao
    abstract fun resumeDao(): ResumeDao

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
                        cleanupClaimedAt INTEGER,
                        generation INTEGER NOT NULL,
                        sessionToken TEXT,
                        sessionClaimedAt INTEGER,
                        retiredStorageKind TEXT,
                        retiredStorageValue TEXT
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

        @Volatile
        private var instance: TransferHistoryDatabase? = null

        fun getInstance(context: Context): TransferHistoryDatabase =
            instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext,
                    TransferHistoryDatabase::class.java,
                    "transfer_history.db"
                ).addMigrations(MIGRATION_1_2)
                    .build()
                    .also { instance = it }
            }
    }
}
