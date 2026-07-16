package com.example.transfer.transfer

import java.io.InputStream
import com.example.transfer.resume.ResumeValidationException

data class SendFileMetadata(
    val length: Long,
    val lastModified: Long?
)

data class SendFileSource(
    val displayName: String,
    val mimeType: String,
    val length: Long,
    val sourceUri: String? = null,
    val lastModified: Long? = null,
    val metadataProvider: () -> SendFileMetadata = {
        SendFileMetadata(length, lastModified)
    },
    val openStream: () -> InputStream
) {
    fun requireUnchanged(): SendFileMetadata {
        val current = try {
            metadataProvider()
        } catch (error: Exception) {
            throw ResumeValidationException("Source is no longer readable", error)
        }
        if (current.length != length ||
            (lastModified != null && current.lastModified != lastModified)
        ) {
            throw ResumeValidationException("Source changed after selection")
        }
        return current
    }
}
