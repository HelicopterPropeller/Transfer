package com.example.transfer.pairing

import android.graphics.Bitmap
import android.graphics.Color

object QrBitmapEncoder {
    fun encode(content: String, size: Int): Bitmap {
        val matrix = QrMatrixEncoder.encode(content, size)
        val pixels = IntArray(matrix.dark.size) { index ->
            if (matrix.dark[index]) Color.BLACK else Color.WHITE
        }
        return Bitmap.createBitmap(pixels, matrix.width, matrix.height, Bitmap.Config.ARGB_8888)
    }
}
