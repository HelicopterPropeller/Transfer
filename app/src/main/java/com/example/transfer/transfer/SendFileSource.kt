package com.example.transfer.transfer

import java.io.InputStream

data class SendFileSource(
    val displayName: String,
    val mimeType: String,
    val length: Long,
    val openStream: () -> InputStream
)
