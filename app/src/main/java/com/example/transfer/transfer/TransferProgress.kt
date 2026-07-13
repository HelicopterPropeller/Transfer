package com.example.transfer.transfer

object TransferProgress {
    fun percent(transferred: Long, total: Long): Int {
        if (total <= 0) return 100
        return ((transferred.coerceAtLeast(0).coerceAtMost(total) * 100) / total).toInt()
    }
}
