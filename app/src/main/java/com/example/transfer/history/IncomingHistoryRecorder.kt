package com.example.transfer.history

import kotlinx.coroutines.CancellationException

interface IncomingTransferHistory {
    suspend fun start(
        fileName: String,
        fileSize: Long,
        mimeType: String,
        peerAddress: String
    ): Long?

    suspend fun succeed(historyId: Long?, receivedUri: String?)

    suspend fun fail(historyId: Long?, errorMessage: String?)

    companion object {
        val None = object : IncomingTransferHistory {
            override suspend fun start(
                fileName: String,
                fileSize: Long,
                mimeType: String,
                peerAddress: String
            ): Long? = null

            override suspend fun succeed(historyId: Long?, receivedUri: String?) = Unit
            override suspend fun fail(historyId: Long?, errorMessage: String?) = Unit
        }
    }
}

class IncomingHistoryRecorder(
    private val store: TransferHistoryStore,
    private val peerResolver: (String) -> HistoryPeer = { address ->
        HistoryPeer(id = null, name = null, address = address)
    }
) : IncomingTransferHistory {
    override suspend fun start(
        fileName: String,
        fileSize: Long,
        mimeType: String,
        peerAddress: String
    ): Long? = bestEffort {
        val peer = peerResolver(peerAddress)
        store.start(
            TransferHistoryDraft(
                direction = TransferDirection.RECEIVE,
                fileName = fileName,
                fileSize = fileSize,
                mimeType = mimeType,
                peerId = peer.id,
                peerName = peer.name,
                peerAddress = peer.address
            )
        )
    }

    override suspend fun succeed(historyId: Long?, receivedUri: String?) {
        if (historyId == null) return
        bestEffort {
            store.finish(
                id = historyId,
                status = TransferHistoryStatus.SUCCESS,
                receivedUri = receivedUri
            )
        }
    }

    override suspend fun fail(historyId: Long?, errorMessage: String?) {
        if (historyId == null) return
        bestEffort {
            store.finish(
                id = historyId,
                status = TransferHistoryStatus.FAILED,
                errorMessage = errorMessage
            )
        }
    }

    private suspend fun <T> bestEffort(block: suspend () -> T): T? = try {
        block()
    } catch (error: CancellationException) {
        throw error
    } catch (_: Exception) {
        null
    }
}
