package com.example.transfer.pairing

import com.google.zxing.BinaryBitmap
import com.google.zxing.MultiFormatReader
import com.google.zxing.RGBLuminanceSource
import com.google.zxing.common.HybridBinarizer
import org.junit.Assert.assertEquals
import org.junit.Test

class QrMatrixEncoderTest {
    @Test
    fun `encoded pairing payload decodes without network services`() {
        val raw = "lantransfer://pair?v=4&id=device&name=Pixel&ip=192.168.1.5&port=42043&token=token&expires=123456"
        val matrix = QrMatrixEncoder.encode(raw, 320)
        val pixels = IntArray(matrix.width * matrix.height) { index ->
            if (matrix.dark[index]) 0xff000000.toInt() else 0xffffffff.toInt()
        }
        val source = RGBLuminanceSource(matrix.width, matrix.height, pixels)

        val decoded = MultiFormatReader().decode(BinaryBitmap(HybridBinarizer(source)))

        assertEquals(raw, decoded.text)
    }
}
