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
    ): BatchTransferResult {
        require(files.isNotEmpty())

        val totalBytes = files.fold(0L) { total, file ->
            require(file.length >= 0) { "File length must not be negative" }
            checkedAdd(total, file.length, "Total file length exceeds Long.MAX_VALUE")
        }
        var completedBytes = 0L
        var successCount = 0
        var lastFileProgress = 100
        val failures = mutableListOf<BatchFailure>()

        files.forEachIndexed { index, file ->
            throwIfCancelled()
            pauseController.awaitBetweenFiles(onPauseState)
            throwIfCancelled()
            lastFileProgress = if (file.length == 0L) 100 else 0
            var confirmedBytes = 0L

            val result = try {
                sendOne(file) { current ->
                    confirmedBytes = current.confirmedBytes.coerceIn(0, file.length)
                    lastFileProgress = current.percent
                    onProgress(
                        BatchTransferProgress(
                            fileIndex = index + 1,
                            fileCount = files.size,
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
                throw exception
            } catch (exception: Exception) {
                Result.failure(exception)
            }

            val failure = result.exceptionOrNull()
            if (failure is CancellationException) throw failure
            throwIfCancelled()

            if (result.isSuccess) {
                completedBytes = checkedAdd(
                    completedBytes,
                    file.length,
                    "Completed byte count exceeds Long.MAX_VALUE"
                )
                successCount++
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
            }
        }

        val lastFile = files.last()
        onProgress(
            BatchTransferProgress(
                fileIndex = files.size,
                fileCount = files.size,
                fileName = lastFile.displayName,
                fileProgress = lastFileProgress,
                batchProgress = 100
            )
        )
        return BatchTransferResult(successCount, failures)
    }

    private fun throwIfCancelled() {
        if (pauseController.state == TransferPauseState.CANCELLED) {
            throw CancellationException("Transfer cancelled")
        }
    }

    private fun checkedAdd(left: Long, right: Long, message: String): Long = try {
        Math.addExact(left, right)
    } catch (exception: ArithmeticException) {
        throw IllegalArgumentException(message, exception)
    }
}
