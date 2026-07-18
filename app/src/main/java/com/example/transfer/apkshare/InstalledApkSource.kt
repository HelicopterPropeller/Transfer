package com.example.transfer.apkshare

import android.content.Context
import android.os.Build
import java.io.File
import java.security.MessageDigest

data class InstalledApkDescriptor(
    val baseApk: File,
    val splitApks: List<File>,
    val versionName: String,
    val versionCode: Long,
)

data class ApkArtifact(
    val file: File,
    val fileName: String,
    val versionName: String,
    val versionCode: Long,
    val size: Long,
    val sha256: String,
)

sealed interface ApkPreparationResult {
    data class Ready(val artifact: ApkArtifact) : ApkPreparationResult

    data object SplitInstallUnsupported : ApkPreparationResult

    data class Failure(val message: String) : ApkPreparationResult
}

class InstalledApkSource {
    fun prepare(context: Context, cacheDirectory: File): ApkPreparationResult {
        val applicationInfo = context.applicationInfo
        val packageInfo = context.packageManager.getPackageInfo(context.packageName, 0)
        val versionName = packageInfo.versionName?.takeIf(String::isNotBlank) ?: "unknown"
        val versionCode = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            packageInfo.longVersionCode
        } else {
            @Suppress("DEPRECATION")
            packageInfo.versionCode.toLong()
        }
        val descriptor = InstalledApkDescriptor(
            baseApk = File(applicationInfo.sourceDir),
            splitApks = applicationInfo.splitSourceDirs.orEmpty().map(::File),
            versionName = versionName,
            versionCode = versionCode,
        )
        return prepare(descriptor, cacheDirectory)
    }

    fun prepare(
        descriptor: InstalledApkDescriptor,
        cacheDirectory: File,
    ): ApkPreparationResult {
        if (descriptor.splitApks.isNotEmpty()) {
            return ApkPreparationResult.SplitInstallUnsupported
        }

        return runCatching {
            require(descriptor.baseApk.isFile && descriptor.baseApk.canRead())
            cacheDirectory.mkdirs()
            val safeVersion = descriptor.versionName.replace(Regex("[^A-Za-z0-9._-]"), "_")
            val target = File(cacheDirectory, "Transfer-$safeVersion.apk")
            descriptor.baseApk.inputStream().use { input ->
                target.outputStream().use(input::copyTo)
            }

            val digest = MessageDigest.getInstance("SHA-256")
            target.inputStream().use { input ->
                val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                while (true) {
                    val read = input.read(buffer)
                    if (read < 0) break
                    digest.update(buffer, 0, read)
                }
            }

            ApkPreparationResult.Ready(
                ApkArtifact(
                    file = target,
                    fileName = target.name,
                    versionName = descriptor.versionName,
                    versionCode = descriptor.versionCode,
                    size = target.length(),
                    sha256 = digest.digest().joinToString("") { "%02x".format(it) },
                ),
            )
        }.getOrElse {
            ApkPreparationResult.Failure(it.message ?: "Unable to prepare installed APK")
        }
    }
}
