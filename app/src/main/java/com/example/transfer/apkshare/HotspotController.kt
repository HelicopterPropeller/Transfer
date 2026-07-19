package com.example.transfer.apkshare

import android.content.Context
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Handler
import android.os.Looper
import androidx.annotation.RequiresApi
import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

enum class HotspotRequirement {
    MANUAL,
    FINE_LOCATION,
    NEARBY_WIFI,
}

object HotspotPermissionPolicy {
    fun requirementFor(api: Int): HotspotRequirement = when {
        api < Build.VERSION_CODES.O -> HotspotRequirement.MANUAL
        api < Build.VERSION_CODES.TIRAMISU -> HotspotRequirement.FINE_LOCATION
        else -> HotspotRequirement.NEARBY_WIFI
    }
}

enum class HotspotFailure {
    USER_REJECTED,
    TETHERING_DISALLOWED,
    INCOMPATIBLE_MODE,
    NO_CHANNEL,
    GENERIC,
    ;

    companion object {
        fun fromPlatform(reason: Int): HotspotFailure = when (reason) {
            WifiManager.LocalOnlyHotspotCallback.ERROR_TETHERING_DISALLOWED -> TETHERING_DISALLOWED
            WifiManager.LocalOnlyHotspotCallback.ERROR_INCOMPATIBLE_MODE -> INCOMPATIBLE_MODE
            WifiManager.LocalOnlyHotspotCallback.ERROR_NO_CHANNEL -> NO_CHANNEL
            else -> GENERIC
        }
    }
}

data class HotspotCredentials(
    val ssid: String,
    val password: String,
    val wifiQrPayload: String,
) {
    companion object {
        fun create(ssid: String, password: String): HotspotCredentials = HotspotCredentials(
            ssid = ssid,
            password = password,
            wifiQrPayload = WifiQrPayload.encode(ssid, password, hidden = false),
        )
    }
}

sealed interface HotspotStartResult {
    data class Started(
        val credentials: HotspotCredentials,
        val before: List<InterfaceAddressSnapshot>,
    ) : HotspotStartResult

    data class ManualRequired(val failure: HotspotFailure) : HotspotStartResult
}

internal class HotspotReservationOwner<T : AutoCloseable> : Closeable {
    private val lock = Any()
    private var reservation: T? = null
    private var closed = false

    fun offer(candidate: T): Boolean {
        val accepted = synchronized(lock) {
            if (closed || reservation != null) {
                false
            } else {
                reservation = candidate
                true
            }
        }
        if (!accepted) runCatching { candidate.close() }
        return accepted
    }

    override fun close() {
        val owned = synchronized(lock) {
            if (closed) return
            closed = true
            reservation.also { reservation = null }
        }
        runCatching { owned?.close() }
    }
}

internal class HotspotResultGate : Closeable {
    private val lock = Any()
    private var closed = false
    private var delivered = false

    val isClosed: Boolean
        get() = synchronized(lock) { closed }

    fun publish(callback: () -> Unit): Boolean = synchronized(lock) {
        if (closed || delivered) return false
        delivered = true
        callback()
        true
    }

    override fun close() {
        synchronized(lock) {
            closed = true
        }
    }
}

class HotspotController(
    context: Context,
    private val wifiManager: WifiManager = context.applicationContext
        .getSystemService(Context.WIFI_SERVICE) as WifiManager,
    private val apiLevel: Int = Build.VERSION.SDK_INT,
    private val addressSnapshot: () -> List<InterfaceAddressSnapshot> = {
        InterfaceAddressProvider.snapshot(context.applicationContext)
    },
    private val callbackHandler: Handler = Handler(Looper.getMainLooper()),
) : Closeable {
    private val reservations = HotspotReservationOwner<WifiManager.LocalOnlyHotspotReservation>()
    private val startRequested = AtomicBoolean()
    private val resultGate = HotspotResultGate()

    @RequiresApi(Build.VERSION_CODES.O)
    fun start(callback: (HotspotStartResult) -> Unit) {
        if (resultGate.isClosed || !startRequested.compareAndSet(false, true)) return
        if (HotspotPermissionPolicy.requirementFor(apiLevel) == HotspotRequirement.MANUAL) {
            publishManual(callback, HotspotFailure.GENERIC)
            return
        }

        val before = addressSnapshot()
        val platformCallback = object : WifiManager.LocalOnlyHotspotCallback() {
            override fun onStarted(reservation: WifiManager.LocalOnlyHotspotReservation) {
                // Offering to a closed owner closes a late reservation immediately.
                if (!reservations.offer(reservation)) return
                val credentials = runCatching { credentialsFrom(reservation) }.getOrNull()
                if (credentials == null) {
                    publishManual(callback, HotspotFailure.GENERIC)
                    close()
                    return
                }
                if (!resultGate.publish { callback(HotspotStartResult.Started(credentials, before)) }) {
                    close()
                }
            }

            override fun onFailed(reason: Int) {
                publishManual(callback, HotspotFailure.fromPlatform(reason))
            }
        }

        try {
            wifiManager.startLocalOnlyHotspot(platformCallback, callbackHandler)
        } catch (_: SecurityException) {
            publishManual(callback, HotspotFailure.GENERIC)
        } catch (_: RuntimeException) {
            publishManual(callback, HotspotFailure.GENERIC)
        }
    }

    override fun close() {
        resultGate.close()
        reservations.close()
    }

    private fun publishManual(
        callback: (HotspotStartResult) -> Unit,
        failure: HotspotFailure,
    ) {
        resultGate.publish { callback(HotspotStartResult.ManualRequired(failure)) }
    }

    @RequiresApi(Build.VERSION_CODES.O)
    private fun credentialsFrom(
        reservation: WifiManager.LocalOnlyHotspotReservation,
    ): HotspotCredentials {
        val raw = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            credentialsFromSoftAp(reservation)
        } else {
            credentialsFromLegacyConfig(reservation)
        }
        val ssid = unquote(raw.first)
        val password = unquote(raw.second)
        require(ssid.isNotBlank() && password.isNotBlank()) { "Hotspot credentials unavailable" }
        return HotspotCredentials.create(ssid, password)
    }

    @RequiresApi(Build.VERSION_CODES.R)
    @Suppress("DEPRECATION")
    private fun credentialsFromSoftAp(
        reservation: WifiManager.LocalOnlyHotspotReservation,
    ): Pair<String, String> {
        val configuration = reservation.softApConfiguration
        return configuration.ssid.orEmpty() to configuration.passphrase.orEmpty()
    }

    @Suppress("DEPRECATION")
    @RequiresApi(Build.VERSION_CODES.O)
    private fun credentialsFromLegacyConfig(
        reservation: WifiManager.LocalOnlyHotspotReservation,
    ): Pair<String, String> {
        val configuration = reservation.wifiConfiguration
        return configuration?.SSID.orEmpty() to configuration?.preSharedKey.orEmpty()
    }

    private fun unquote(value: String): String = value
        .takeIf { it.length >= 2 && it.first() == '"' && it.last() == '"' }
        ?.substring(1, value.length - 1)
        ?: value
}
