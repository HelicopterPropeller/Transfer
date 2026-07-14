package com.example.transfer.history

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.TextView
import androidx.core.view.isVisible
import androidx.recyclerview.widget.DiffUtil
import androidx.recyclerview.widget.ListAdapter
import androidx.recyclerview.widget.RecyclerView
import com.example.transfer.R
import com.google.android.material.button.MaterialButton

class HistoryAdapter(
    private val onPrimaryAction: (HistoryItemUi) -> Unit,
    private val onDelete: (HistoryItemUi) -> Unit
) : ListAdapter<HistoryItemUi, HistoryAdapter.HistoryViewHolder>(DIFF) {
    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): HistoryViewHolder {
        val view = LayoutInflater.from(parent.context)
            .inflate(R.layout.item_transfer_history, parent, false)
        return HistoryViewHolder(view)
    }

    override fun onBindViewHolder(holder: HistoryViewHolder, position: Int) {
        holder.bind(getItem(position))
    }

    inner class HistoryViewHolder(itemView: View) : RecyclerView.ViewHolder(itemView) {
        private val directionText: TextView = itemView.findViewById(R.id.historyDirectionText)
        private val statusText: TextView = itemView.findViewById(R.id.historyStatusText)
        private val fileNameText: TextView = itemView.findViewById(R.id.historyFileNameText)
        private val metadataText: TextView = itemView.findViewById(R.id.historyMetadataText)
        private val startedText: TextView = itemView.findViewById(R.id.historyStartedText)
        private val errorText: TextView = itemView.findViewById(R.id.historyErrorText)
        private val primaryButton: MaterialButton = itemView.findViewById(R.id.historyPrimaryButton)
        private val deleteButton: MaterialButton = itemView.findViewById(R.id.historyDeleteButton)

        fun bind(item: HistoryItemUi) {
            directionText.text = item.directionText
            statusText.text = item.statusText
            fileNameText.text = item.fileName
            metadataText.text = item.metadataText
            startedText.text = item.startedText
            errorText.text = item.errorText.orEmpty()
            errorText.isVisible = item.errorText != null
            primaryButton.isVisible = item.showResend || item.showOpen
            primaryButton.setText(if (item.showOpen) R.string.open_file else R.string.resend_file)
            primaryButton.setOnClickListener { onPrimaryAction(item) }
            deleteButton.setOnClickListener { onDelete(item) }
        }
    }

    private companion object {
        val DIFF = object : DiffUtil.ItemCallback<HistoryItemUi>() {
            override fun areItemsTheSame(oldItem: HistoryItemUi, newItem: HistoryItemUi): Boolean =
                oldItem.id == newItem.id

            override fun areContentsTheSame(oldItem: HistoryItemUi, newItem: HistoryItemUi): Boolean =
                oldItem == newItem
        }
    }
}
