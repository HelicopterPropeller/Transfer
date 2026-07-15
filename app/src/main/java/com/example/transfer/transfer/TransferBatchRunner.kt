package com.example.transfer.transfer

import java.util.concurrent.CancellationException

data class BatchTransferProgress(
    val fileIndex: Int,
    val fileCount: Int,
    val fileName: String,
    val fileProgress: Int,
    val batchProgress: Int
)

data class BatchFailure(
    val fileName: String,
    val message: String
)

data class BatchTransferResult(
    val successCount: Int,
    val failures: List<BatchFailure>
)

data class BatchTransferItem(
    val source: SendFileSource,
    val initialConfirmedBytes: Long = 0
)

fun formatBatchCompletion(result: BatchTransferResult): String {
    val summary = "发送完成：成功 ${result.successCount}，失败 ${result.failures.size}"
    if (result.failures.isEmpty()) return summary
    val shown = result.failures.take(FAILURE_SUMMARY_LIMIT)
    val details = shown.joinToString("；") { "${it.fileName}: ${it.message}" }
    val remaining = result.failures.size - shown.size
    val suffix = if (remaining > 0) "；另有 $remaining 项" else ""
    return "$summary；$details$suffix"
}

private const val FAILURE_SUMMARY_LIMIT = 2

class TransferBatchRunner(
    private val pauseController: TransferPauseController,
    private val sendOne: suspend (
        SendFileSource,
        (FileTransferProgress) -> Unit
    ) -> Result<Unit>
) {
    suspend fun run(
        files: List<SendFileSource>,
        onProgress: (BatchTransferProgress) -> Unit,
        onPauseState: (TransferPauseState) -> Unit
    ): BatchTransferResult = runItems(
        files.map(::BatchTransferItem), onProgress, onPauseState
    )

    suspend fun runItems(
        items: List<BatchTransferItem>,
        onProgress: (BatchTransferProgress) -> Unit,
        onPauseState: (TransferPauseState) -> Unit
    ): BatchTransferResult {
        require(items.isNotEmpty())

        val totalBytes = items.fold(0L) { total, item ->
            val file = item.source
            require(file.length >= 0) { "File length must not be negative" }
            require(item.initialConfirmedBytes in 0..file.length) {
                "Initial confirmed bytes must be within the file"
            }
            checkedAdd(total, file.length, "Total file length exceeds Long.MAX_VALUE")
        }
        var completedBytes = 0L
        var successCount = 0
        var lastFileProgress = 100
        var lastFileSucceeded = false
        val failures = mutableListOf<BatchFailure>()

        items.forEachIndexed { index, item ->
            val file = item.source
            throwIfCancelled()
            pauseController.awaitBetweenFiles(onPauseState)
            throwIfCancelled()
            lastFileProgress = if (file.length == 0L) 100 else 0
            var confirmedBytes = item.initialConfirmedBytes
            lastFileProgress = FileTransferProgress(confirmedBytes, file.length).percent
            if (confirmedBytes > 0L) {
                onProgress(
                    BatchTransferProgress(
                        fileIndex = index + 1,
                        fileCount = items.size,
                        fileName = file.displayName,
                        fileProgress = lastFileProgress,
                        batchProgress = TransferProgress.percent(
                            checkedAdd(completedBytes, confirmedBytes, "Batch progress exceeds Long.MAX_VALUE"),
                            totalBytes
                        )
                    )
                )
            }

            val result = try {
                sendOne(file) { current ->
                    confirmedBytes = current.confirmedBytes.coerceIn(0, file.length)
                    lastFileProgress = current.percent
                    onProgress(
                        BatchTransferProgress(
                            fileIndex = index + 1,
                            fileCount = items.size,
                            fileName = file.displayName,
                            fileProgress = current.percent,
                            batchProgress = TransferProgress.percent(
                                checkedAdd(
                                    completedBytes,
                                    confirmedBytes,
                                    "Batch progress exceeds Long.MAX_VALUE"
                                ),
                                totalBytes
                            )
                        )
                    )
                }
            } catch (exception: CancellationException) {
                onProgress(nonSuccessProgress(
                    index, items.size, file, completedBytes, confirmedBytes, totalBytes
                ))
                throw exception
            } catch (exception: Exception) {
                Result.failure(exception)
            }

            val failure = result.exceptionOrNull()
            if (failure is CancellationException) {
                onProgress(nonSuccessProgress(
                    index, items.size, file, completedBytes, confirmedBytes, totalBytes
                ))
                throw failure
            }
            if (pauseController.state == TransferPauseState.CANCELLED) {
                onProgress(nonSuccessProgress(
                    index, items.size, file, completedBytes, confirmedBytes, totalBytes
                ))
                throw CancellationException("Transfer cancelled")
            }

            if (result.isSuccess) {
                completedBytes = checkedAdd(
                    completedBytes,
                    file.length,
                    "Completed byte count exceeds Long.MAX_VALUE"
                )
                successCount++
                lastFileSucceeded = true
            } else {
                completedBytes = checkedAdd(
                    completedBytes,
                    confirmedBytes,
                    "Completed byte count exceeds Long.MAX_VALUE"
                )
                failures += BatchFailure(
                    fileName = file.displayName,
                    message = failure?.message ?: "发送失败"
                )
                lastFileSucceeded = false
            }
        }

        val lastFile = items.last().source
        val byteBatchProgress = TransferProgress.percent(completedBytes, totalBytes)
        onProgress(
            BatchTransferProgress(
                fileIndex = items.size,
                fileCount = items.size,
                fileName = lastFile.displayName,
                fileProgress = if (lastFileSucceeded) lastFileProgress else nonSuccessPercent(lastFileProgress),
                batchProgress = if (failures.isEmpty()) byteBatchProgress else nonSuccessPercent(byteBatchProgress)
            )
        )
        return BatchTransferResult(successCount, failures)
    }

    private fun throwIfCancelled() {
        if (pauseController.state == TransferPauseState.CANCELLED) {
            throw CancellationException("Transfer cancelled")
        }
    }

    private fun nonSuccessProgress(
        index: Int,
        fileCount: Int,
        file: SendFileSource,
        completedBytes: Long,
        confirmedBytes: Long,
        totalBytes: Long
    ): BatchTransferProgress = BatchTransferProgress(
        fileIndex = index + 1,
        fileCount = fileCount,
        fileName = file.displayName,
        fileProgress = nonSuccessPercent(FileTransferProgress(confirmedBytes, file.length).percent),
        batchProgress = nonSuccessPercent(TransferProgress.percent(
            checkedAdd(completedBytes, confirmedBytes, "Batch progress exceeds Long.MAX_VALUE"),
            totalBytes
        ))
    )

    private fun nonSuccessPercent(percent: Int): Int = percent.coerceAtMost(99)

    private fun checkedAdd(left: Long, right: Long, message: String): Long = try {
        Math.addExact(left, right)
    } catch (exception: ArithmeticException) {
        throw IllegalArgumentException(message, exception)
    }
}
