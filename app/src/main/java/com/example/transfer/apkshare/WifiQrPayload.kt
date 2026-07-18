package com.example.transfer.apkshare

object WifiQrPayload {
    fun encode(ssid: String, password: String, hidden: Boolean): String =
        "WIFI:T:WPA;S:${escape(ssid)};P:${escape(password)};H:$hidden;;"

    private fun escape(value: String): String = buildString {
        value.forEach { char ->
            if (char in listOf('\\', ';', ',', ':')) append('\\')
            append(char)
        }
    }
}
