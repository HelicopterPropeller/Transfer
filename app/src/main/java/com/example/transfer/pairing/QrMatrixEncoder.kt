package com.example.transfer.pairing

import com.google.zxing.BarcodeFormat
import com.google.zxing.EncodeHintType
import com.google.zxing.qrcode.QRCodeWriter
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel

data class QrMatrix(val width: Int, val height: Int, val dark: BooleanArray)

object QrMatrixEncoder {
    fun encode(content: String, size: Int): QrMatrix {
        require(content.isNotBlank())
        require(size > 0)
        val bits = QRCodeWriter().encode(
            content,
            BarcodeFormat.QR_CODE,
            size,
            size,
            mapOf(
                EncodeHintType.CHARACTER_SET to "UTF-8",
                EncodeHintType.ERROR_CORRECTION to ErrorCorrectionLevel.M,
                EncodeHintType.MARGIN to 2
            )
        )
        return QrMatrix(
            width = bits.width,
            height = bits.height,
            dark = BooleanArray(bits.width * bits.height) { index ->
                bits[index % bits.width, index / bits.width]
            }
        )
    }
}
