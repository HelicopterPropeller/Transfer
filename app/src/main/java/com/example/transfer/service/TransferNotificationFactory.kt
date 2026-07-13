package com.example.transfer.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.os.Build
import androidx.core.app.NotificationCompat
import com.example.transfer.MainActivity
import com.example.transfer.R

class TransferNotificationFactory(private val context: Context) {
    fun createChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(CHANNEL_ID, "局域网传输", NotificationManager.IMPORTANCE_LOW)
            channel.description = "显示局域网文件发送和接收进度"
            context.getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
        }
    }

    fun build(model: TransferNotificationModel): Notification {
        val openIntent = PendingIntent.getActivity(
            context, 0,
            Intent(context, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val stopIntent = PendingIntent.getService(
            context, 1,
            Intent(context, TransferForegroundService::class.java).setAction(TransferForegroundService.ACTION_STOP),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_transfer_notification)
            .setContentTitle(model.title)
            .setContentText(model.text)
            .setContentIntent(openIntent)
            .setOnlyAlertOnce(true)
            .setOngoing(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .addAction(0, "停止服务", stopIntent)
        if (model.showProgress) builder.setProgress(100, model.progress, false)
        else builder.setProgress(0, 0, false)
        return builder.build()
    }

    companion object {
        const val CHANNEL_ID = "transfer_service"
        const val NOTIFICATION_ID = 42042
    }
}
