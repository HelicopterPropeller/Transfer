package com.example.transfer.apkshare

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.os.Build
import androidx.core.app.NotificationCompat
import com.example.transfer.R

class ApkShareNotificationFactory(private val context: Context) {
    fun createChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val channel = NotificationChannel(
            CHANNEL_ID,
            context.getString(R.string.apk_share_channel_name),
            NotificationManager.IMPORTANCE_LOW,
        ).apply {
            description = context.getString(R.string.apk_share_channel_description)
        }
        context.getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
    }

    fun build(state: ApkShareState): Notification {
        val contentIntent = PendingIntent.getActivity(
            context,
            0,
            Intent().setClassName(context, "${context.packageName}.apkshare.ApkShareActivity")
                .addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
        val cancelIntent = PendingIntent.getService(
            context,
            1,
            Intent(context, ApkShareForegroundService::class.java)
                .setAction(ApkShareForegroundService.ACTION_CANCEL),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
        val builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_transfer_notification)
            .setContentTitle(context.getString(R.string.apk_share_notification_title))
            .setContentText(notificationText(state))
            .setContentIntent(contentIntent)
            .setOnlyAlertOnce(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setOngoing(state !is ApkShareState.Completed && state !is ApkShareState.Cancelled)

        when (state) {
            ApkShareState.PreparingApk,
            ApkShareState.StartingHotspot,
            -> builder.setProgress(0, 0, true)

            is ApkShareState.Downloading -> builder.setProgress(100, state.progressPercent, false)
            else -> builder.setProgress(0, 0, false)
        }
        if (state !is ApkShareState.Completed && state !is ApkShareState.Cancelled) {
            builder.addAction(0, context.getString(R.string.cancel), cancelIntent)
        }
        return builder.build()
    }

    private fun notificationText(state: ApkShareState): String = context.getString(
        when (state) {
            ApkShareState.Idle -> R.string.apk_share_notification_idle
            ApkShareState.PreparingApk -> R.string.apk_share_notification_preparing
            is ApkShareState.PermissionRequired -> R.string.apk_share_notification_permission
            ApkShareState.StartingHotspot -> R.string.apk_share_notification_hotspot
            is ApkShareState.JoinHotspot -> R.string.apk_share_notification_join_hotspot
            is ApkShareState.ReadyToDownload -> R.string.apk_share_notification_ready
            is ApkShareState.Downloading -> R.string.apk_share_notification_downloading
            ApkShareState.Completed -> R.string.apk_share_notification_completed
            is ApkShareState.ManualHotspotRequired -> R.string.apk_share_notification_manual_hotspot
            is ApkShareState.Error -> R.string.apk_share_notification_error
            ApkShareState.Cancelled -> R.string.apk_share_notification_cancelled
        },
    )

    companion object {
        const val CHANNEL_ID = "apk_share_service"
        const val NOTIFICATION_ID = 42044
    }
}
