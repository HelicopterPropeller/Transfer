package com.example.transfer.protocol

import java.io.IOException

class ProtocolException(message: String) : IOException(message)

object TransferProtocol {
    const val MAX_FILE_SIZE = 10L * 1024 * 1024 * 1024
    const val CHUNK_SIZE = 1_048_576
    const val ACK = 0
    const val NACK = 1
    const val FATAL = 2
    const val CONTROL_ACK = 3
    const val COMPLETE = 4
    const val FAILED = 1
    const val SUCCESS = 0
    const val FAILURE = FAILED
}
