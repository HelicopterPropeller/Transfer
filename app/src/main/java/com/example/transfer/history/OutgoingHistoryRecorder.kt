package com.example.transfer.history

import com.example.transfer.transfer.SendFileSource
import com.example.transfer.transfer.TransferPauseController
import com.example.transfer.transfer.TransferPauseState
import java.util.concurrent.CancellationException

data class HistoryPeer(
    val id: String?,
    val name: String?,
    val address: String?
)

class OutgoingHistoryRecorder(
    private val store: TransferHistoryStore
) {
    suspend fun send(
        source: SendFileSource,
        peer: HistoryPeer,
        controller: TransferPauseController,
        block: suspend () -> Result<Unit>
    ): Result<Unit> {
        val historyId = store.start(
            TransferHistoryDraft(
                direction = TransferDirection.SEND,
                fileName = source.displayName,
                fileSize = source.length,
                mimeType = source.mimeType,
                peerId = peer.id,
                peerName = peer.name,
                peerAddress = peer.address,
                sourceUri = source.sourceUri
            )
        )

        val result = try {
            block()
        } catch (exception: CancellationException) {
            finish(historyId, TransferHistoryStatus.CANCELLED, exception.message)
            throw exception
        } catch (exception: Exception) {
            finish(historyId, TransferHistoryStatus.FAILED, exception.message)
            throw exception
        }

        val terminal = when {
            result.isSuccess -> TransferHistoryStatus.SUCCESS
            controller.state == TransferPauseState.CANCELLED -> TransferHistoryStatus.CANCELLED
            else -> TransferHistoryStatus.FAILED
        }
        finish(historyId, terminal, result.exceptionOrNull()?.message)
        return result
    }

    private suspend fun finish(
        historyId: Long?,
        status: TransferHistoryStatus,
        errorMessage: String?
    ) {
        if (historyId != null) store.finish(historyId, status, errorMessage)
    }
}
