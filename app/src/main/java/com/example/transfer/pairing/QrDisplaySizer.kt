package com.example.transfer.pairing

object QrDisplaySizer {
    fun side(availableWidthPx: Int, maximumPx: Int, fallbackPx: Int): Int {
        require(maximumPx > 0 && fallbackPx > 0)
        val requested = if (availableWidthPx > 0) availableWidthPx else fallbackPx
        return requested.coerceAtMost(maximumPx).coerceAtLeast(1)
    }
}
