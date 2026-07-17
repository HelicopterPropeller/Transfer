package com.example.transfer.pairing

import com.example.transfer.protocol.ProtocolException
import com.example.transfer.protocol.readBoundedText
import com.example.transfer.protocol.writeBoundedText
import java.io.DataInputStream
import java.io.DataOutputStream

data class PairingRequest(
    val token: String,
    val senderDeviceId: String,
    val senderDeviceName: String
)

fun interface PairingRequestHandler {
    fun handle(request: PairingRequest): PairingResponse
}

enum class PairingStatus(val code: Int) {
    ACCEPTED(0),
    REJECTED(1),
    PROTOCOL_ERROR(2)
}

data class PairingResponse(
    val status: PairingStatus,
    val receiverDeviceId: String? = null,
    val receiverDeviceName: String? = null,
    val tcpPort: Int? = null
) {
    init {
        if (status == PairingStatus.ACCEPTED) {
            require(!receiverDeviceId.isNullOrBlank())
            require(!receiverDeviceName.isNullOrBlank())
            require(tcpPort != null && tcpPort in 1..65535)
        } else {
            require(receiverDeviceId == null && receiverDeviceName == null && tcpPort == null)
        }
    }

    companion object {
        fun accepted(deviceId: String, deviceName: String, tcpPort: Int) =
            PairingResponse(PairingStatus.ACCEPTED, deviceId, deviceName, tcpPort)
    }
}

object PairingProtocol {
    const val MAX_TOKEN_BYTES = 128
    const val MAX_DEVICE_ID_BYTES = 64
    const val MAX_DEVICE_NAME_BYTES = 128

    fun writeRequest(output: DataOutputStream, request: PairingRequest) {
        validateText(request.token, MAX_TOKEN_BYTES, "pairing token")
        validateText(request.senderDeviceId, MAX_DEVICE_ID_BYTES, "sender device ID")
        validateText(request.senderDeviceName, MAX_DEVICE_NAME_BYTES, "sender device name")
        output.writeBoundedText(request.token, MAX_TOKEN_BYTES, "pairing token")
        output.writeBoundedText(request.senderDeviceId, MAX_DEVICE_ID_BYTES, "sender device ID")
        output.writeBoundedText(request.senderDeviceName, MAX_DEVICE_NAME_BYTES, "sender device name")
    }

    fun readRequest(input: DataInputStream): PairingRequest = PairingRequest(
        token = input.readBoundedText(MAX_TOKEN_BYTES, "pairing token"),
        senderDeviceId = input.readBoundedText(MAX_DEVICE_ID_BYTES, "sender device ID"),
        senderDeviceName = input.readBoundedText(MAX_DEVICE_NAME_BYTES, "sender device name")
    ).also {
        validateReadText(it.token, "pairing token")
        validateReadText(it.senderDeviceId, "sender device ID")
        validateReadText(it.senderDeviceName, "sender device name")
    }

    fun writeResponse(output: DataOutputStream, response: PairingResponse) {
        output.writeByte(response.status.code)
        if (response.status == PairingStatus.ACCEPTED) {
            val id = requireNotNull(response.receiverDeviceId)
            val name = requireNotNull(response.receiverDeviceName)
            validateText(id, MAX_DEVICE_ID_BYTES, "receiver device ID")
            validateText(name, MAX_DEVICE_NAME_BYTES, "receiver device name")
            output.writeBoundedText(id, MAX_DEVICE_ID_BYTES, "receiver device ID")
            output.writeBoundedText(name, MAX_DEVICE_NAME_BYTES, "receiver device name")
            output.writeInt(requireNotNull(response.tcpPort))
        }
    }

    fun readResponse(input: DataInputStream): PairingResponse {
        val statusCode = input.readUnsignedByte()
        val status = PairingStatus.entries.firstOrNull { it.code == statusCode }
            ?: throw ProtocolException("Unknown pairing status")
        if (status != PairingStatus.ACCEPTED) return PairingResponse(status)
        val id = input.readBoundedText(MAX_DEVICE_ID_BYTES, "receiver device ID")
        val name = input.readBoundedText(MAX_DEVICE_NAME_BYTES, "receiver device name")
        validateReadText(id, "receiver device ID")
        validateReadText(name, "receiver device name")
        val port = input.readInt()
        if (port !in 1..65535) throw ProtocolException("Invalid pairing port")
        return PairingResponse.accepted(id, name, port)
    }

    private fun validateText(value: String, maximum: Int, label: String) {
        require(value.isNotBlank() && '\u0000' !in value &&
            value.toByteArray(Charsets.UTF_8).size in 1..maximum
        ) { "Invalid $label" }
    }

    private fun validateReadText(value: String, label: String) {
        if (value.isBlank() || '\u0000' in value) throw ProtocolException("Invalid $label")
    }
}
