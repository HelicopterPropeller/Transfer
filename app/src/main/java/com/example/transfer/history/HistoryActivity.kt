package com.example.transfer.history

import android.content.Intent
import android.os.Bundle
import android.view.View
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.appcompat.app.AppCompatActivity
import androidx.core.net.toUri
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.example.transfer.MainActivity
import com.example.transfer.R
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
            .setMessage(R.string.delete_history_message)
            .setNegativeButton(R.string.cancel, null)
            .setPositiveButton(R.string.delete_record) { _, _ -> viewModel.delete(item.id) }
            .show()
    }

    private fun confirmClear() {
        MaterialAlertDialogBuilder(this)
            .setTitle(R.string.clear_history_title)
            .setMessage(R.string.clear_history_message)
            .setNegativeButton(R.string.cancel, null)
            .setPositiveButton(R.string.clear_history) { _, _ -> viewModel.clear() }
            .show()
    }

    private fun performPrimaryAction(item: HistoryItemUi) {
        if (item.showResend) {
            startActivity(
                Intent(this, MainActivity::class.java)
                    .putExtra(MainActivity.EXTRA_RESEND_URI, item.sourceUri)
                    .putExtra(MainActivity.EXTRA_RESEND_NAME, item.fileName)
                    .putExtra(MainActivity.EXTRA_RESEND_MIME_TYPE, item.mimeType)
                    .putExtra(MainActivity.EXTRA_RESEND_SIZE, item.fileSize)
            )
            return
        }
        val intent = Intent(Intent.ACTION_VIEW).apply {
            setDataAndType(item.receivedUri.orEmpty().toUri(), item.mimeType)
            addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
        }
        val launched = runCatching {
            startActivity(intent)
        }.isSuccess
        if (!launched) {
            Snackbar.make(
                findViewById(R.id.historyRoot),
                R.string.history_action_unavailable,
                Snackbar.LENGTH_SHORT
            ).show()
        }
    }
}
