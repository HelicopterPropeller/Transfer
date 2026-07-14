package com.example.transfer.history

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.util.Locale
import java.util.TimeZone

data class HistoryUiState(
    val items: List<HistoryItemUi> = emptyList(),
    val empty: Boolean = true,
    val errorMessage: String? = null
)

class HistoryViewModel(application: Application) : AndroidViewModel(application) {
    private val store: TransferHistoryStore = TransferHistoryRepository(
        TransferHistoryDatabase.getInstance(application).transferHistoryDao()
    )
    private val mutableState = MutableStateFlow(HistoryUiState())
    val state: StateFlow<HistoryUiState> = mutableState.asStateFlow()

    init {
        viewModelScope.launch {
            store.observeAll()
                .catch { error ->
                    if (error is CancellationException) throw error
                    publishError("无法加载传输历史")
                }
                .collect { entries ->
                    val locale = Locale.getDefault()
                    val timeZone = TimeZone.getDefault()
                    val items = entries.map { entry -> HistoryItemUi.from(entry, locale, timeZone) }
                    mutableState.update { current ->
                        current.copy(items = items, empty = items.isEmpty())
                    }
                }
        }
    }

    fun delete(id: Long) {
        viewModelScope.launch {
            mutate("删除历史记录失败") { store.delete(id) }
        }
    }

    fun clear() {
        viewModelScope.launch {
            mutate("清空历史记录失败") { store.clear() }
        }
    }

    fun consumeError() {
        mutableState.update { current -> current.copy(errorMessage = null) }
    }

    private suspend fun mutate(message: String, action: suspend () -> Boolean) {
        val succeeded = try {
            action()
        } catch (error: CancellationException) {
            throw error
        } catch (_: Exception) {
            false
        }
        if (!succeeded) publishError(message)
    }

    private fun publishError(message: String) {
        mutableState.update { current -> current.copy(errorMessage = message) }
    }
}
