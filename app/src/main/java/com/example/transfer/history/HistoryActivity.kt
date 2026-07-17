package com.example.transfer.history

import android.content.Intent
import android.os.Bundle
import android.view.View
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.content.ContextCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.example.transfer.MainActivity
import com.example.transfer.R
import com.example.transfer.service.TransferForegroundService
import com.google.android.material.appbar.MaterialToolbar
import com.google.android.material.button.MaterialButton
import com.google.android.material.dialog.MaterialAlertDialogBuilder
import com.google.android.material.snackbar.Snackbar
import kotlinx.coroutines.launch

class HistoryActivity : AppCompatActivity() {
    private val viewModel: HistoryViewModel by viewModels()
    private lateinit var adapter: HistoryAdapter
    private lateinit var emptyView: View
    private lateinit var clearButton: MaterialButton
    private var latestState = HistoryUiState()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContentView(R.layout.activity_history)
        val root = findViewById<View>(R.id.historyRoot)
        ViewCompat.setOnApplyWindowInsetsListener(root) { view, insets ->
            val bars = insets.getInsets(WindowInsetsCompat.Type.systemBars())
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            insets
        }

        findViewById<MaterialToolbar>(R.id.historyToolbar).apply {
            setNavigationIcon(android.R.drawable.ic_menu_close_clear_cancel)
            setNavigationOnClickListener { finish() }
        }
        clearButton = findViewById(R.id.clearHistoryButton)
        clearButton.setOnClickListener { confirmClear() }
        emptyView = findViewById(R.id.emptyHistoryText)
        adapter = HistoryAdapter(::performPrimaryAction, ::confirmDelete)
        findViewById<RecyclerView>(R.id.historyList).apply {
            layoutManager = LinearLayoutManager(this@HistoryActivity)
            adapter = this@HistoryActivity.adapter
        }

        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                viewModel.state.collect(::render)
            }
        }
    }

    private fun render(state: HistoryUiState) {
        latestState = state
        adapter.submitList(state.items)
        emptyView.visibility = if (state.empty) View.VISIBLE else View.GONE
        clearButton.isEnabled = !state.empty
        state.errorMessage?.let { message ->
            Snackbar.make(findViewById(R.id.historyRoot), message, Snackbar.LENGTH_SHORT).show()
            viewModel.consumeError()
        }
    }

    private fun confirmDelete(item: HistoryItemUi) {
        MaterialAlertDialogBuilder(this)
            .setTitle(R.string.delete_history_title)
            .setMessage(if (item.isActiveOutgoing) {
                R.string.delete_active_history_message
            } else {
                R.string.delete_history_message
            })
            .setNegativeButton(R.string.cancel, null)
            .setPositiveButton(R.string.delete_record) { _, _ ->
                HistoryMutationPolicy.mutate(
                    cancelActiveOutgoing = item.isActiveOutgoing,
                    cancel = ::requestOutgoingCancellation,
                    mutation = { viewModel.delete(item.id) }
                )
            }
            .show()
    }

    private fun confirmClear() {
        MaterialAlertDialogBuilder(this)
            .setTitle(R.string.clear_history_title)
            .setMessage(if (latestState.hasActiveOutgoing) {
                R.string.clear_active_history_message
            } else {
                R.string.clear_history_message
            })
            .setNegativeButton(R.string.cancel, null)
            .setPositiveButton(R.string.clear_history) { _, _ ->
                HistoryMutationPolicy.mutate(
                    cancelActiveOutgoing = latestState.hasActiveOutgoing,
                    cancel = ::requestOutgoingCancellation,
                    mutation = viewModel::clear
                )
            }
            .show()
    }

    private fun requestOutgoingCancellation() {
        ContextCompat.startForegroundService(
            this,
            Intent(this, TransferForegroundService::class.java)
                .setAction(TransferForegroundService.ACTION_CANCEL)
        )
    }

    private fun performPrimaryAction(item: HistoryItemUi) {
        if (item.showResend) {
            resend(item)
            return
        }
        HistoryFileActions.open(this, item)?.let(::showActionError)
    }

    private fun resend(item: HistoryItemUi) {
        val sourceUri = item.sourceUri?.takeIf(String::isNotBlank) ?: return
        val retryIntent = HistoryRetryContract.write(
            Intent(this, MainActivity::class.java),
            sourceUri = sourceUri,
            preferredPeerId = item.peerId
        ).addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP)
        startActivity(retryIntent)
    }

    private fun showActionError(message: String) {
        Snackbar.make(findViewById(R.id.historyRoot), message, Snackbar.LENGTH_SHORT).show()
    }
}
