package com.example.transfer.history

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase

@Database(
    entities = [TransferHistoryEntity::class],
    version = 1,
    exportSchema = false
)
abstract class TransferHistoryDatabase : RoomDatabase() {
    abstract fun transferHistoryDao(): TransferHistoryDao

    companion object {
        @Volatile
        private var instance: TransferHistoryDatabase? = null

        fun getInstance(context: Context): TransferHistoryDatabase =
            instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext,
                    TransferHistoryDatabase::class.java,
                    "transfer_history.db"
                ).build().also { instance = it }
            }
    }
}
