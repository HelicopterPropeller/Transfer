package com.example.transfer.history

import android.content.Intent

data class HistoryRetryRequest(
    val sourceUri: String,
    val preferredPeerId: String?
)

object HistoryRetryContract {
    private const val EXTRA_SOURCE_URI = "com.example.transfer.extra.HISTORY_SOURCE_URI"
    private const val EXTRA_PREFERRED_PEER_ID = "com.example.transfer.extra.PREFERRED_PEER_ID"

    fun write(intent: Intent, sourceUri: String, preferredPeerId: String?): Intent =
        intent.putExtra(EXTRA_SOURCE_URI, sourceUri).apply {
            val preferred = preferredPeerId?.takeIf(String::isNotBlank)
            if (preferred == null) {
                removeExtra(EXTRA_PREFERRED_PEER_ID)
            } else {
                putExtra(EXTRA_PREFERRED_PEER_ID, preferred)
            }
        }

    fun read(intent: Intent): HistoryRetryRequest? {
        val sourceUri = intent.getStringExtra(EXTRA_SOURCE_URI)
            ?.takeIf(String::isNotBlank)
            ?: return null
        return HistoryRetryRequest(
            sourceUri = sourceUri,
            preferredPeerId = intent.getStringExtra(EXTRA_PREFERRED_PEER_ID)
                ?.takeIf(String::isNotBlank)
        )
    }

    fun clear(intent: Intent) {
        intent.removeExtra(EXTRA_SOURCE_URI)
        intent.removeExtra(EXTRA_PREFERRED_PEER_ID)
    }
}
