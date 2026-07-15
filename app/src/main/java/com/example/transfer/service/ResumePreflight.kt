package com.example.transfer.service

import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferStartMode
import java.util.concurrent.atomic.AtomicLong

enum class ResumeChoice { RESUME_AVAILABLE, RESTART_ALL, CANCEL }

data class ResumePrompt(
    val id: Long,
    val resumableFileNames: List<String>,
    val fileCount: Int
)

internal data class ResumePreflightFile<T>(
    val value: T,
    val fileName: String,
    val status: ResumeStatus
)

internal data class ResumeSelectedFile<T>(
    val value: T,
    val status: ResumeStatus,
    val mode: TransferStartMode
)

internal sealed interface ResumePreflightResult<out T> {
    data class Ready<T>(val files: List<ResumeSelectedFile<T>>) : ResumePreflightResult<T>
    data class Waiting(val prompt: ResumePrompt) : ResumePreflightResult<Nothing>
    data object Busy : ResumePreflightResult<Nothing>
    data object Ignored : ResumePreflightResult<Nothing>
}

internal sealed interface ResumeConfirmation<out T> {
    data class Ready<T>(val files: List<ResumeSelectedFile<T>>) : ResumeConfirmation<T>
    data object Busy : ResumeConfirmation<Nothing>
    data object Cancelled : ResumeConfirmation<Nothing>
    data object Ignored : ResumeConfirmation<Nothing>
}

internal class ResumePreflight<T> {
    private sealed interface Slot<out T> {
        data class Reserved(val token: Long) : Slot<Nothing>
        data class Waiting<T>(val prompt: ResumePrompt, val files: List<ResumePreflightFile<T>>) : Slot<T>
    }

    private val lock = Any()
    private val ids = AtomicLong(System.currentTimeMillis())
    private var slot: Slot<T>? = null

    val prompt: ResumePrompt?
        get() = synchronized(lock) { (slot as? Slot.Waiting)?.prompt }

    val hasPendingBatch: Boolean
        get() = synchronized(lock) { slot != null }

    fun reserve(isBusy: () -> Boolean = { false }): Long? = synchronized(lock) {
        if (slot != null || isBusy()) return@synchronized null
        ids.incrementAndGet().also { slot = Slot.Reserved(it) }
    }

    fun finish(
        token: Long,
        files: List<ResumePreflightFile<T>>,
        acquireBusy: () -> Boolean = { true }
    ): ResumePreflightResult<T> =
        synchronized(lock) {
            val reserved = slot as? Slot.Reserved
            if (reserved?.token != token) return@synchronized ResumePreflightResult.Ignored
            val resumable = files.filter { it.status.state == ResumeState.AVAILABLE }
            if (resumable.isEmpty()) {
                val selected = select(files, ResumeChoice.RESUME_AVAILABLE)
                val acquired = acquireBusy()
                slot = null
                if (acquired) ResumePreflightResult.Ready(selected)
                else ResumePreflightResult.Busy
            } else {
                val prompt = ResumePrompt(
                    id = token,
                    resumableFileNames = resumable.map { it.fileName },
                    fileCount = files.size
                )
                slot = Slot.Waiting(prompt, files)
                ResumePreflightResult.Waiting(prompt)
            }
        }

    fun cancelReservation(token: Long): Boolean = synchronized(lock) {
        val matches = when (val current = slot) {
            is Slot.Reserved -> current.token == token
            is Slot.Waiting -> current.prompt.id == token
            null -> false
        }
        if (!matches) return@synchronized false
        slot = null
        true
    }

    fun confirm(
        promptId: Long,
        choice: ResumeChoice,
        acquireBusy: () -> Boolean = { true }
    ): ResumeConfirmation<T> = synchronized(lock) {
        val waiting = slot as? Slot.Waiting<T>
        if (waiting?.prompt?.id != promptId) return@synchronized ResumeConfirmation.Ignored
        if (choice == ResumeChoice.CANCEL) {
            slot = null
            ResumeConfirmation.Cancelled
        } else {
            val selected = select(waiting.files, choice)
            val acquired = acquireBusy()
            slot = null
            if (acquired) ResumeConfirmation.Ready(selected) else ResumeConfirmation.Busy
        }
    }

    fun clear() = synchronized(lock) { slot = null }

    private fun select(
        files: List<ResumePreflightFile<T>>,
        choice: ResumeChoice
    ): List<ResumeSelectedFile<T>> = files.map { file ->
        val mode = when (file.status.state) {
            ResumeState.NONE -> TransferStartMode.NEW
            ResumeState.INVALID -> TransferStartMode.RESTART
            ResumeState.AVAILABLE -> if (choice == ResumeChoice.RESUME_AVAILABLE) {
                TransferStartMode.RESUME
            } else {
                TransferStartMode.RESTART
            }
        }
        ResumeSelectedFile(file.value, file.status, mode)
    }
}

internal fun publishResumePromptSafely(
    token: Long,
    prompt: ResumePrompt,
    cancelPending: (Long) -> Boolean,
    publishPrompt: (ResumePrompt) -> Unit,
    clearPublicPrompt: (Long) -> Unit,
    publishError: (Throwable) -> Unit
): Boolean = try {
    publishPrompt(prompt)
    true
} catch (error: Exception) {
    cancelPending(token)
    runCatching { clearPublicPrompt(token) }
    runCatching { publishError(error) }
    false
}
