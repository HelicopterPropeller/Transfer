package com.example.transfer.transfer

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

        val totalBytes = files.sumOf(SendFileSource::length)
        var completedBytes = 0L
        var successCount = 0
        var lastFileProgress = 100
        val failures = mutableListOf<BatchFailure>()

        files.forEachIndexed { index, file ->
            pauseController.awaitBetweenFiles(onPauseState)
            lastFileProgress = if (file.length == 0L) 100 else 0

            val result = sendOne(file) { current ->
                lastFileProgress = current.percent
                onProgress(
                    BatchTransferProgress(
                        fileIndex = index + 1,
                        fileCount = files.size,
                        fileName = file.displayName,
                        fileProgress = current.percent,
                        batchProgress = TransferProgress.percent(
                            completedBytes + current.confirmedBytes,
                            totalBytes
                        )
                    )
                )
            }

            completedBytes += file.length
            if (result.isSuccess) {
                successCount++
            } else {
                failures += BatchFailure(
                    fileName = file.displayName,
                    message = result.exceptionOrNull()?.message ?: "发送失败"
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
}
