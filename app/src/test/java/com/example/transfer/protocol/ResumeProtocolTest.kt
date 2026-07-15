package com.example.transfer.protocol

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.util.UUID

class ResumeProtocolTest {
    @Test
    fun `offer round trips bounded metadata`() {
        val expected = validOffer()

        val actual = ResumeProtocol.readOffer(DataInputStream(ByteArrayInputStream(encodeOffer(expected))))

        assertEquals(expected, actual)
        assertEquals(1_048_576, actual.chunkSize)
    }

    @Test
    fun `offer rejects invalid identifiers and empty metadata`() {
        listOf(
            validOffer().copy(transferId = "not-a-uuid"),
            validOffer().copy(senderDeviceId = ""),
            validOffer().copy(fileName = ""),
            validOffer().copy(mimeType = "")
        ).forEach { offer ->
            assertThrows(ProtocolException::class.java) {
                ResumeProtocol.writeOffer(DataOutputStream(ByteArrayOutputStream()), offer)
            }
        }
    }

    @Test
    fun `offer rejects metadata beyond utf8 byte limits`() {
        listOf(
            validOffer().copy(senderDeviceId = "x".repeat(ResumeProtocol.MAX_ID_BYTES + 1)),
            validOffer().copy(fileName = "x".repeat(ResumeProtocol.MAX_FILE_NAME_BYTES + 1)),
            validOffer().copy(mimeType = "x".repeat(ResumeProtocol.MAX_MIME_BYTES + 1))
        ).forEach { offer ->
            assertThrows(ProtocolException::class.java) {
                ResumeProtocol.writeOffer(DataOutputStream(ByteArrayOutputStream()), offer)
            }
        }
    }

    @Test
    fun `offer rejects wrong chunk size and file larger than ten gib`() {
        assertThrows(ProtocolException::class.java) {
            ResumeProtocol.writeOffer(
                DataOutputStream(ByteArrayOutputStream()),
                validOffer().copy(chunkSize = 1024)
            )
        }
        assertThrows(ProtocolException::class.java) {
            ResumeProtocol.writeOffer(
                DataOutputStream(ByteArrayOutputStream()),
                validOffer().copy(fileSize = TransferProtocol.MAX_FILE_SIZE + 1)
            )
        }
    }

    @Test
    fun `status round trips every resume state`() {
        ResumeState.entries.forEach { state ->
            val expected = validStatus(state)

            val actual = ResumeProtocol.readStatus(DataInputStream(ByteArrayInputStream(encodeStatus(expected))))

            assertEquals(expected.state, actual.state)
            assertEquals(expected.confirmedBytes, actual.confirmedBytes)
            assertEquals(expected.nextChunkIndex, actual.nextChunkIndex)
            assertArrayEquals(expected.chainDigest, actual.chainDigest)
            assertArrayEquals(expected.lastChunkHash, actual.lastChunkHash)
        }
    }

    @Test
    fun `status rejects invalid offset index and digest lengths`() {
        listOf(
            validStatus().copy(confirmedBytes = -1),
            validStatus().copy(nextChunkIndex = 2),
            validStatus().copy(chainDigest = ByteArray(ChunkCodec.DIGEST_SIZE - 1)),
            validStatus().copy(lastChunkHash = ByteArray(ChunkCodec.DIGEST_SIZE + 1))
        ).forEach { status ->
            assertThrows(ProtocolException::class.java) {
                ResumeProtocol.writeStatus(DataOutputStream(ByteArrayOutputStream()), status)
            }
        }
    }

    @Test
    fun `status read with offer rejects offset beyond file and non-final partial chunk`() {
        val offer = validOffer().copy(fileSize = TransferProtocol.CHUNK_SIZE.toLong() * 2 + 10)

        assertThrows(ProtocolException::class.java) {
            ResumeProtocol.readStatus(
                DataInputStream(ByteArrayInputStream(encodeStatus(validStatus(confirmedBytes = offer.fileSize + 1)))),
                offer
            )
        }
        assertThrows(ProtocolException::class.java) {
            ResumeProtocol.readStatus(
                DataInputStream(ByteArrayInputStream(encodeStatus(validStatus(confirmedBytes = 10)))),
                offer
            )
        }

        val finalStatus = validStatus(confirmedBytes = offer.fileSize)
        val actual = ResumeProtocol.readStatus(
            DataInputStream(ByteArrayInputStream(encodeStatus(finalStatus))),
            offer
        )
        assertEquals(offer.fileSize, actual.confirmedBytes)
    }

    @Test
    fun `start mode round trips every reserved value`() {
        TransferStartMode.entries.forEach { mode ->
            val bytes = ByteArrayOutputStream().also {
                ResumeProtocol.writeStartMode(DataOutputStream(it), mode)
            }.toByteArray()

            assertEquals(mode, ResumeProtocol.readStartMode(DataInputStream(ByteArrayInputStream(bytes))))
        }
    }

    @Test
    fun `unknown status and start mode codes are rejected`() {
        assertThrows(ProtocolException::class.java) {
            ResumeProtocol.readStatus(DataInputStream(ByteArrayInputStream(byteArrayOf(99))))
        }
        assertThrows(ProtocolException::class.java) {
            ResumeProtocol.readStartMode(DataInputStream(ByteArrayInputStream(byteArrayOf(99))))
        }
    }

    private fun validOffer() = TransferOffer(
        transferId = UUID.randomUUID().toString(),
        senderDeviceId = "sender-device",
        fileName = "photo.jpg",
        mimeType = "image/jpeg",
        fileSize = 987654321L
    )

    private fun validStatus(
        state: ResumeState = ResumeState.AVAILABLE,
        confirmedBytes: Long = TransferProtocol.CHUNK_SIZE.toLong()
    ) = ResumeStatus(
        state = state,
        confirmedBytes = confirmedBytes,
        nextChunkIndex = if (confirmedBytes == 0L) 0 else
            ((confirmedBytes - 1) / TransferProtocol.CHUNK_SIZE + 1).toInt(),
        chainDigest = ByteArray(ChunkCodec.DIGEST_SIZE) { 1 },
        lastChunkHash = ByteArray(ChunkCodec.DIGEST_SIZE) { 2 }
    )

    private fun encodeOffer(offer: TransferOffer): ByteArray = ByteArrayOutputStream().also {
        ResumeProtocol.writeOffer(DataOutputStream(it), offer)
    }.toByteArray()

    private fun encodeStatus(status: ResumeStatus): ByteArray = ByteArrayOutputStream().also {
        ResumeProtocol.writeStatus(DataOutputStream(it), status)
    }.toByteArray()
}
