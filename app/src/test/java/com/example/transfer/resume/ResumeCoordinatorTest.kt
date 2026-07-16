package com.example.transfer.resume

import com.example.transfer.protocol.ChunkCodec
import com.example.transfer.protocol.DigestChain
import com.example.transfer.protocol.PrefixDigestScanner
import com.example.transfer.protocol.ResumeState
import com.example.transfer.protocol.ResumeStatus
import com.example.transfer.protocol.TransferOffer
import com.example.transfer.protocol.TransferProtocol
import com.example.transfer.protocol.TransferStartMode
import com.example.transfer.storage.ResumableFileHandle
import com.example.transfer.storage.ResumableIncomingFileStore
import com.example.transfer.storage.StoredFileLocation
import com.example.transfer.transfer.SendFileSource
import com.example.transfer.transfer.SendFileMetadata
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CancellationException
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.IOException
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class ResumeCoordinatorTest {
    private val now = 1_000_000L

    @Test
    fun `prepare rejects source whose current size differs from selection metadata`() = runBlocking {
        val store = FakeResumeStore()
        val coordinator = coordinator(store)
        val stale = source(length = 3L, currentLength = 4L)

        val failure = runCatching {
            coordinator.prepareOutgoing(stale, "peer-1", "sender-1")
        }.exceptionOrNull()

        assertTrue(failure is ResumeValidationException)
        assertTrue(store.outgoing.isEmpty())
    }

    @Test
    fun `prepare rejects source whose current modification time changed`() = runBlocking {
        val coordinator = coordinator()
        val stale = source(lastModified = 42L, currentLastModified = 43L)

        val failure = runCatching {
            coordinator.prepareOutgoing(stale, "peer-1", "sender-1")
        }.exceptionOrNull()

        assertTrue(failure is ResumeValidationException)
    }

    @Test
    fun `stable source identity reuses transfer id`() = runBlocking {
        val store = FakeResumeStore()
        val coordinator = coordinator(store)
        val source = source(lastModified = 42L)

        val first = coordinator.prepareOutgoing(source, "peer-1", "sender-1")
        val second = coordinator.prepareOutgoing(source, "peer-1", "sender-1")

        assertEquals(first.offer.transferId, second.offer.transferId)
        assertEquals(ResumeState.NONE, second.resumeStatus.state)
        assertEquals(42L, store.outgoing.values.single().lastModified)
    }

    @Test
    fun `changed source size or last modified replaces stale transfer id`() = runBlocking {
        val store = FakeResumeStore()
        val coordinator = coordinator(store)
        val first = coordinator.prepareOutgoing(source(length = 3L, lastModified = 42L), "peer-1", "sender-1")

        val changedSize = coordinator.prepareOutgoing(
            source(bytes = byteArrayOf(1, 2, 3, 4), length = 4L, lastModified = 42L),
            "peer-1",
            "sender-1"
        )
        val changedModified = coordinator.prepareOutgoing(
            source(bytes = byteArrayOf(1, 2, 3, 4), length = 4L, lastModified = 43L),
            "peer-1",
            "sender-1"
        )

        assertNotEquals(first.offer.transferId, changedSize.offer.transferId)
        assertNotEquals(changedSize.offer.transferId, changedModified.offer.transferId)
        assertEquals(setOf(first.offer.transferId, changedSize.offer.transferId), store.deletedOutgoing.toSet())
    }

    @Test
    fun `concurrent outgoing preparation returns one unique key owner`() {
        val store = FakeResumeStore()
        val start = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val futures = listOf("t1", "t2").map { id ->
                pool.submit<String> {
                    start.await()
                    runBlocking {
                        ResumeCoordinator(
                            store, FakeFiles(), clock = { now }, newTransferId = { id }
                        ).prepareOutgoing(source(), "peer-1", "sender-1").offer.transferId
                    }
                }
            }
            start.countDown()
            val ids = futures.map { it.get() }
            assertEquals(1, ids.toSet().size)
            assertEquals(ids.singleOrNull() ?: ids.first(), store.outgoing.values.single().transferId)
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `incoming metadata mismatch is an invalid candidate`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 4L)
        store.saveIncoming(checkpoint(offer, files.put("t1", byteArrayOf(1, 2, 3, 4))).copy(fileSize = 5L))

        val status = coordinator.queryIncoming(offer)

        assertEquals(ResumeState.INVALID, status.state)
    }

    @Test
    fun `structurally corrupt checkpoint is an invalid candidate`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 4L)
        store.saveIncoming(
            checkpoint(offer, files.put("t1", byteArrayOf(1, 2, 3, 4))).copy(
                confirmedBytes = 1L,
                nextChunkIndex = 7,
                chainDigest = byteArrayOf(1)
            )
        )

        assertEquals(ResumeState.INVALID, coordinator.queryIncoming(offer).state)
    }

    @Test
    fun `resume truncates receiver bytes beyond committed prefix`() = runBlocking {
        val bytes = ByteArray(TransferProtocol.CHUNK_SIZE + 9) { (it % 251).toByte() }
        val confirmed = TransferProtocol.CHUNK_SIZE.toLong()
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = bytes.size.toLong())
        val location = files.put(offer.transferId, bytes)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(bytes), confirmed)
        store.saveIncoming(checkpoint(offer, location, prefix))
        val expected = coordinator.queryIncoming(offer)

        val session = coordinator.openIncoming(offer, TransferStartMode.RESUME, expected)

        assertEquals(confirmed, session.handle.length())
        assertEquals(confirmed, session.prefix.scannedBytes)
        assertTrue(coordinator.validatePrefix(expected, session.prefix))
    }

    @Test
    fun `resume rejects receiver shorter than committed prefix`() = runBlocking {
        val committedBytes = ByteArray(TransferProtocol.CHUNK_SIZE) { 7 }
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = committedBytes.size.toLong() + 1)
        val location = files.put(offer.transferId, committedBytes.copyOf(committedBytes.size - 1))
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(committedBytes), committedBytes.size.toLong())
        store.saveIncoming(checkpoint(offer, location, prefix))

        assertThrows<ResumeValidationException> {
            coordinator.openIncoming(offer, TransferStartMode.RESUME, coordinator.queryIncoming(offer))
        }
        Unit
    }

    @Test
    fun `missing receiver partial releases acquired ownership`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 1L)
        val missing = StoredFileLocation("FAKE", "missing")
        store.saveIncoming(checkpoint(offer, missing))

        assertThrows<ResumeValidationException> {
            coordinator.openIncoming(offer, TransferStartMode.RESUME, coordinator.queryIncoming(offer))
        }

        assertNull(store.findIncoming(offer.transferId)?.sessionToken)
    }

    @Test
    fun `sender or receiver prefix chain mismatch is invalid`() = runBlocking {
        val receiver = PrefixDigestScanner.scan(ByteArrayInputStream(byteArrayOf(1, 2, 3)), 3, 3)
        val sender = PrefixDigestScanner.scan(ByteArrayInputStream(byteArrayOf(1, 2, 4)), 3, 3)
        val status = ResumeStatus(
            state = ResumeState.AVAILABLE,
            confirmedBytes = receiver.scannedBytes,
            nextChunkIndex = receiver.nextChunkIndex,
            chainDigest = receiver.chainDigest,
            lastChunkHash = receiver.lastChunkHash
        )

        assertFalse(coordinator().validatePrefix(status, sender))
        assertTrue(coordinator().validatePrefix(status, receiver))
    }

    @Test
    fun `resume rejects corrupt receiver prefix`() = runBlocking {
        val original = ByteArray(TransferProtocol.CHUNK_SIZE) { 3 }
        val corrupt = original.copyOf().also { it[0] = 4 }
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = original.size.toLong() + 1)
        val location = files.put(offer.transferId, corrupt)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(original), original.size.toLong())
        store.saveIncoming(checkpoint(offer, location, prefix))

        assertEquals(ResumeState.INVALID, coordinator.queryIncoming(offer).state)

        assertThrows<ResumeValidationException> {
            coordinator.openIncoming(offer, TransferStartMode.RESUME, coordinator.queryIncoming(offer))
        }
        Unit
    }

    @Test
    fun `corrupt query invalid can restart after repeated invalid validation`() = runBlocking {
        val original = ByteArray(TransferProtocol.CHUNK_SIZE) { 3 }
        val corrupt = original.copyOf().also { it[0] = 4 }
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = original.size.toLong() + 1)
        val location = files.put(offer.transferId, corrupt)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(original), original.size.toLong())
        store.saveIncoming(checkpoint(offer, location, prefix))
        val invalid = coordinator.queryIncoming(offer)

        val restarted = coordinator.openIncoming(offer, TransferStartMode.RESTART, invalid)

        assertEquals(ResumeState.INVALID, invalid.state)
        assertEquals(0L, restarted.checkpoint.confirmedBytes)
        assertTrue(location in files.deleted)
    }

    @Test
    fun `missing partial query invalid can restart`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 3)
        val missing = StoredFileLocation("FAKE", "missing")
        store.saveIncoming(checkpoint(offer, missing))
        val invalid = coordinator.queryIncoming(offer)

        val restarted = coordinator.openIncoming(offer, TransferStartMode.RESTART, invalid)

        assertEquals(ResumeState.INVALID, invalid.state)
        assertEquals(0L, restarted.checkpoint.confirmedBytes)
    }

    @Test
    fun `restart refuses invalid status when receiver prefix becomes valid after query`() = runBlocking {
        val original = ByteArray(TransferProtocol.CHUNK_SIZE) { 3 }
        val corrupt = original.copyOf().also { it[0] = 4 }
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = original.size.toLong() + 1)
        val location = files.put(offer.transferId, corrupt)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(original), original.size.toLong())
        store.saveIncoming(checkpoint(offer, location, prefix))
        val invalid = coordinator.queryIncoming(offer)
        files.replace(location, original)

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESTART, invalid)
        }
        assertTrue(files.deleted.isEmpty())
    }

    @Test
    fun `query propagates transient storage io instead of returning invalid`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles().apply { openInputFailure = IOException("storage busy") }
        val coordinator = coordinator(store, files)
        val offer = offer()
        val location = files.put(offer.transferId, ByteArray(0))
        store.saveIncoming(checkpoint(offer, location))

        val failure = assertThrows<IOException> { coordinator.queryIncoming(offer) }

        assertEquals("storage busy", failure.message)
    }

    @Test
    fun `query propagates storage security failure`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles().apply { openInputFailure = SecurityException("permission revoked") }
        val coordinator = coordinator(store, files)
        val offer = offer()
        val location = files.put(offer.transferId, ByteArray(0))
        store.saveIncoming(checkpoint(offer, location))

        assertThrows<SecurityException> { coordinator.queryIncoming(offer) }
        Unit
    }

    @Test
    fun `cold start recovery clears active ownership dated in the future`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer()
        val location = files.put(offer.transferId, ByteArray(0))
        store.saveIncoming(
            checkpoint(offer, location).copy(
                operationState = IncomingOperationState.ACTIVE,
                sessionToken = "dead-process",
                sessionClaimedAt = now + 365L * 24 * 60 * 60 * 1000
            )
        )

        coordinator.recoverInterruptedState(Long.MAX_VALUE)

        val recovered = store.findIncoming(offer.transferId)
        assertEquals(IncomingOperationState.IDLE, recovered?.operationState)
        assertNull(recovered?.sessionToken)
    }

    @Test
    fun `restart leases then deletes old partial and creates zero checkpoint`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 9L)
        val old = files.put(offer.transferId, byteArrayOf(1, 2, 3))
        store.saveIncoming(checkpoint(offer, old))
        val expected = coordinator.queryIncoming(offer)

        val session = coordinator.openIncoming(offer, TransferStartMode.RESTART, expected)

        assertEquals(listOf(old), files.deleted)
        assertEquals(0L, session.checkpoint.confirmedBytes)
        assertEquals(0, session.checkpoint.nextChunkIndex)
        assertArrayEquals(ByteArray(ChunkCodec.DIGEST_SIZE), session.checkpoint.chainDigest)
        assertTrue(store.expiryUpdates.isNotEmpty())
    }

    @Test
    fun `claimed checkpoint fails closed before restart deletes partial`() = runBlocking {
        val store = FakeResumeStore().apply { allowExpiryUpdate = false }
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer))
        }
        assertTrue(files.deleted.isEmpty())
    }

    @Test
    fun `checkpoint deleted during lease refresh fails with conflict`() = runBlocking {
        val store = FakeResumeStore().apply { removeAfterExpiryUpdate = true }
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer))
        }
        assertTrue(files.deleted.isEmpty())
    }

    @Test
    fun `checkpoint advanced during lease refresh fails before resume touches partial`() = runBlocking {
        val store = FakeResumeStore().apply { advanceAfterExpiryUpdate = true }
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 1L)
        val location = files.put(offer.transferId, ByteArray(0))
        store.saveIncoming(checkpoint(offer, location))

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESUME, coordinator.queryIncoming(offer))
        }
        assertTrue(files.deleted.isEmpty())
        assertEquals(0, files.reopenCount)
    }

    @Test
    fun `commit forces bytes before conditional checkpoint advance`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 1L)
        val session = coordinator.openIncoming(offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        session.handle.writeAt(0, byteArrayOf(8), 1)
        store.forceObserved = { (session.handle as FakeHandle).forced }

        val committed = coordinator.commitChunk(
            session = session,
            confirmedBytes = 1L,
            nextChunkIndex = 1,
            chainDigest = DigestChain.next(
                session.checkpoint.chainDigest, 0, 1, ByteArray(ChunkCodec.DIGEST_SIZE) { 2 }
            ),
            lastChunkHash = ByteArray(ChunkCodec.DIGEST_SIZE) { 2 }
        )

        assertTrue((session.handle as FakeHandle).forced)
        assertEquals(now + ResumeCoordinator.RETENTION_MILLIS, committed.checkpoint.expiresAt)
        assertEquals(1L, committed.checkpoint.confirmedBytes)
    }

    @Test
    fun `commit rejects non final short chunk before force`() = runBlocking {
        val store = FakeResumeStore()
        val coordinator = coordinator(store, FakeFiles())
        val session = coordinator.openIncoming(
            offer(fileSize = 2L),
            TransferStartMode.NEW,
            ResumeStatus(ResumeState.NONE)
        )

        assertThrows<ResumeValidationException> {
            coordinator.commitChunk(
                session, 1L, 1,
                ByteArray(ChunkCodec.DIGEST_SIZE),
                ByteArray(ChunkCodec.DIGEST_SIZE)
            )
        }
        assertFalse((session.handle as FakeHandle).forced)
    }

    @Test
    fun `commit rejects every non contiguous or malformed transition`() = runBlocking {
        val coordinator = coordinator()
        val session = coordinator.openIncoming(
            offer(fileSize = TransferProtocol.CHUNK_SIZE.toLong() + 1),
            TransferStartMode.NEW,
            ResumeStatus(ResumeState.NONE)
        )
        val digest = ByteArray(ChunkCodec.DIGEST_SIZE)
        val invalid = listOf(
            arrayOf(0L, 1, digest, digest),
            arrayOf(1L, 2, digest, digest),
            arrayOf(TransferProtocol.CHUNK_SIZE.toLong() + 1, 1, digest, digest),
            arrayOf(1L, 1, digest, digest),
            arrayOf(TransferProtocol.CHUNK_SIZE.toLong(), 1, byteArrayOf(1), digest),
            arrayOf(TransferProtocol.CHUNK_SIZE.toLong(), 1, digest, byteArrayOf(1))
        )
        invalid.forEach { values ->
            assertThrows<ResumeValidationException> {
                coordinator.commitChunk(
                    session,
                    values[0] as Long,
                    values[1] as Int,
                    values[2] as ByteArray,
                    values[3] as ByteArray
                )
            }
        }
        assertFalse((session.handle as FakeHandle).forced)
    }

    @Test
    fun `duplicate same status resume acquisition has one winner`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer(fileSize = 1L)
        store.saveIncoming(checkpoint(offer, files.put(offer.transferId, ByteArray(0))))
        val status = coordinator.queryIncoming(offer)

        val winner = coordinator.openIncoming(offer, TransferStartMode.RESUME, status)

        assertTrue(winner.sessionToken.isNotBlank())
        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESUME, status)
        }
        Unit
    }

    @Test
    fun `restart create failure preserves old checkpoint and partial`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles().apply { failCreate = true }
        val coordinator = coordinator(store, files)
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        val original = checkpoint(offer, old)
        store.saveIncoming(original)

        assertThrows<IllegalStateException> {
            coordinator.openIncoming(offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer))
        }

        assertEquals(old, store.findIncoming(offer.transferId)?.location?.let { StoredFileLocation(it.kind, it.value) })
        assertTrue(files.contains(old))
    }

    @Test
    fun `new insert conflict deletes staged file without replacing existing row`() = runBlocking {
        val store = FakeResumeStore().apply { allowInsert = false }
        val files = FakeFiles()
        val offer = offer()

        assertThrows<CheckpointConflictException> {
            coordinator(store, files).openIncoming(offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        }

        assertEquals(1, files.deleted.size)
        assertTrue(store.incoming.isEmpty())
    }

    @Test
    fun `new insert conflict keeps journal when staged delete fails until recovery`() = runBlocking {
        val store = FakeResumeStore().apply { allowInsert = false }
        val files = FakeFiles().apply { failDeleteValues += "${offer().transferId}-1" }
        val coordinator = coordinator(store, files)

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer(), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        }

        assertEquals(1, store.stagingJournals.size)
        assertEquals(1, files.stagingCount())

        files.failDeleteValues.clear()
        coordinator.recoverInterruptedState(Long.MAX_VALUE)

        assertTrue(store.stagingJournals.isEmpty())
        assertEquals(0, files.stagingCount())
    }

    @Test
    fun `create partial failure keeps journal when cleanup delete fails until recovery`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles().apply {
            failAfterCreate = true
            failDeleteValues += "${offer().transferId}-1"
        }
        val coordinator = coordinator(store, files)

        assertThrows<IllegalStateException> {
            coordinator.openIncoming(offer(), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        }

        assertEquals(1, store.stagingJournals.size)
        assertEquals(1, files.stagingCount())

        files.failAfterCreate = false
        files.failDeleteValues.clear()
        coordinator.recoverInterruptedState(Long.MAX_VALUE)
        assertTrue(store.stagingJournals.isEmpty())
        assertEquals(0, files.stagingCount())
    }

    @Test
    fun `restart replacement conflict deletes staged file and preserves old ownership`() = runBlocking {
        val store = FakeResumeStore().apply { allowReplace = false }
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer))
        }

        assertTrue(files.contains(old))
        assertEquals(old.value, store.findIncoming(offer.transferId)?.storageValue)
        assertTrue(files.deleted.any { it != old })
    }

    @Test
    fun `restart replacement conflict retains failed cleanup journal and releases old session`() = runBlocking {
        val store = FakeResumeStore().apply { allowReplace = false }
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))
        files.failDeleteValues += "${offer.transferId}-1"

        assertThrows<CheckpointConflictException> {
            coordinator.openIncoming(offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer))
        }

        assertNull(store.findIncoming(offer.transferId)?.sessionToken)
        assertEquals(IncomingOperationState.IDLE, store.findIncoming(offer.transferId)?.operationState)
        assertEquals(1, store.stagingJournals.size)
        assertEquals(1, files.stagingCount())

        files.failDeleteValues.clear()
        coordinator.recoverInterruptedState(Long.MAX_VALUE)
        assertTrue(store.stagingJournals.isEmpty())
        assertEquals(0, files.stagingCount())
        assertTrue(files.contains(old))
    }

    @Test
    fun `restart releases ownership when staging journal insert throws`() = runBlocking {
        val store = FakeResumeStore().apply {
            journalInsertFailure = IllegalStateException("journal insert failed")
        }
        val files = FakeFiles()
        val offer = offer()
        store.saveIncoming(checkpoint(offer, files.put(offer.transferId, byteArrayOf(1))))

        val error = assertThrows<IllegalStateException> {
            coordinator(store, files).openIncoming(
                offer, TransferStartMode.RESTART, coordinator(store, files).queryIncoming(offer)
            )
        }

        assertEquals("journal insert failed", error.message)
        assertNull(store.findIncoming(offer.transferId)?.sessionToken)
        assertEquals(IncomingOperationState.IDLE, store.findIncoming(offer.transferId)?.operationState)
    }

    @Test
    fun `restart releases ownership when staging journal insert is cancelled`() = runBlocking {
        val store = FakeResumeStore().apply {
            journalInsertFailure = CancellationException("journal insert cancelled")
        }
        val files = FakeFiles()
        val offer = offer()
        store.saveIncoming(checkpoint(offer, files.put(offer.transferId, byteArrayOf(1))))

        val error = assertThrows<CancellationException> {
            coordinator(store, files).openIncoming(
                offer, TransferStartMode.RESTART, coordinator(store, files).queryIncoming(offer)
            )
        }

        assertEquals("journal insert cancelled", error.message)
        assertNull(store.findIncoming(offer.transferId)?.sessionToken)
        assertEquals(IncomingOperationState.IDLE, store.findIncoming(offer.transferId)?.operationState)
    }

    @Test
    fun `restart false staging journal conflict releases ownership`() = runBlocking {
        val store = FakeResumeStore().apply { allowJournalInsert = false }
        val files = FakeFiles()
        val offer = offer()
        store.saveIncoming(checkpoint(offer, files.put(offer.transferId, byteArrayOf(1))))

        assertThrows<CheckpointConflictException> {
            coordinator(store, files).openIncoming(
                offer, TransferStartMode.RESTART, coordinator(store, files).queryIncoming(offer)
            )
        }

        assertNull(store.findIncoming(offer.transferId)?.sessionToken)
        assertEquals(IncomingOperationState.IDLE, store.findIncoming(offer.transferId)?.operationState)
    }

    @Test
    fun `restart journal and ownership release failures are aggregated`() = runBlocking {
        val store = FakeResumeStore().apply {
            journalInsertFailure = IllegalStateException("journal insert failed")
            releaseFailureAfterClear = IllegalArgumentException("release failed")
        }
        val files = FakeFiles()
        val offer = offer()
        store.saveIncoming(checkpoint(offer, files.put(offer.transferId, byteArrayOf(1))))

        val error = assertThrows<IllegalStateException> {
            coordinator(store, files).openIncoming(
                offer, TransferStartMode.RESTART, coordinator(store, files).queryIncoming(offer)
            )
        }

        assertEquals("journal insert failed", error.message)
        assertEquals("release failed", error.suppressed.single().message)
        assertNull(store.findIncoming(offer.transferId)?.sessionToken)
        assertEquals(IncomingOperationState.IDLE, store.findIncoming(offer.transferId)?.operationState)
    }

    @Test
    fun `release clears ownership even when handle close fails`() = runBlocking {
        val store = FakeResumeStore()
        val coordinator = coordinator(store, FakeFiles())
        val session = coordinator.openIncoming(
            offer(), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE)
        )
        (session.handle as FakeHandle).failClose = true

        val error = assertThrows<IllegalStateException> { coordinator.releaseIncoming(session) }

        assertEquals("close failed", error.message)
        assertNull(store.findIncoming(session.checkpoint.transferId)?.sessionToken)
        assertEquals(IncomingOperationState.IDLE, store.findIncoming(session.checkpoint.transferId)?.operationState)
    }

    @Test
    fun `release aggregates close and ownership release failures`() = runBlocking {
        val store = FakeResumeStore().apply { allowRelease = false }
        val session = coordinator(store, FakeFiles()).openIncoming(
            offer(), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE)
        )
        (session.handle as FakeHandle).failClose = true

        val error = assertThrows<IllegalStateException> {
            coordinator(store, FakeFiles()).releaseIncoming(session)
        }

        assertEquals("close failed", error.message)
        assertEquals(1, error.suppressed.size)
        assertTrue(error.suppressed.single() is CheckpointConflictException)
    }

    @Test
    fun `stale session cannot commit or complete after ownership moves`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val first = coordinator.openIncoming(
            offer(fileSize = 1L), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE)
        )
        coordinator.releaseIncoming(first)
        val second = coordinator.openIncoming(
            offer(fileSize = 1L), TransferStartMode.RESUME, coordinator.queryIncoming(offer(fileSize = 1L))
        )

        assertThrows<CheckpointConflictException> {
            val lastHash = ByteArray(32)
            coordinator.commitChunk(
                first, 1L, 1,
                DigestChain.next(first.checkpoint.chainDigest, 0, 1, lastHash),
                lastHash
            )
        }
        assertThrows<CheckpointConflictException> { coordinator.completeIncoming(first) }
        assertEquals(second.sessionToken, store.findIncoming(second.checkpoint.transferId)?.sessionToken)
    }

    @Test
    fun `restart delete failure persists retired location and later resume recovers it`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))
        files.failDeleteValues += old.value

        val restarted = coordinator.openIncoming(
            offer,
            TransferStartMode.RESTART,
            coordinator.queryIncoming(offer)
        )

        assertEquals(old.value, restarted.checkpoint.retiredStorageValue)
        coordinator.releaseIncoming(restarted)
        files.failDeleteValues.clear()
        val resumed = coordinator.openIncoming(
            offer,
            TransferStartMode.RESUME,
            coordinator.queryIncoming(offer)
        )
        assertNull(resumed.checkpoint.retiredStorageValue)
        assertFalse(files.contains(old))
    }

    @Test
    fun `restart cancellation after checkpoint switch closes handle and releases replacement ownership`() = runBlocking {
        val store = FakeResumeStore().apply {
            journalDeleteFailure = CancellationException("cancel after switch")
        }
        val files = FakeFiles().apply { failCreatedHandleClose = true }
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))
        val coordinator = coordinator(store, files)

        val error = assertThrows<CancellationException> {
            coordinator.openIncoming(
                offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer)
            )
        }

        assertEquals("cancel after switch", error.message)
        assertEquals("close failed", error.suppressed.single().message)
        assertTrue(files.createdHandles.single().isClosed)
        val replacement = requireNotNull(store.findIncoming(offer.transferId))
        assertEquals(1L, replacement.generation)
        assertNull(replacement.sessionToken)
        assertEquals(IncomingOperationState.IDLE, replacement.operationState)
        assertNull(replacement.retiredStorageValue)
        assertFalse(files.contains(old))
        assertEquals(1, store.stagingJournals.size)
    }

    @Test
    fun `restart cancellation while deleting retired file keeps durable recovery state`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        files.cancelDeleteValues += old.value
        store.saveIncoming(checkpoint(offer, old))
        val coordinator = coordinator(store, files)

        val error = assertThrows<CancellationException> {
            coordinator.openIncoming(
                offer, TransferStartMode.RESTART, coordinator.queryIncoming(offer)
            )
        }

        assertEquals("delete cancelled", error.message)
        assertTrue(files.createdHandles.single().isClosed)
        val replacement = requireNotNull(store.findIncoming(offer.transferId))
        assertNull(replacement.sessionToken)
        assertEquals(IncomingOperationState.IDLE, replacement.operationState)
        assertEquals(old.value, replacement.retiredStorageValue)
        assertEquals(1, store.stagingJournals.size)
        assertTrue(files.contains(old))
    }

    @Test
    fun `process reopen retries retired cleanup without a staging journal`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val offer = offer()
        val retired = files.put("retired-no-journal", byteArrayOf(1))
        val current = files.put("current-no-journal", ByteArray(0))
        store.saveIncoming(
            checkpoint(offer, current).copy(
                generation = 2,
                retiredStorageKind = retired.kind,
                retiredStorageValue = retired.value
            )
        )

        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)

        assertFalse(files.contains(retired))
        assertNull(store.findIncoming(offer.transferId)?.retiredStorageValue)
        assertTrue(files.contains(current))
    }

    @Test
    fun `stale commit index throws checkpoint conflict`() = runBlocking {
        val store = FakeResumeStore().apply { allowCommit = false }
        val coordinator = coordinator(store, FakeFiles())
        val session = coordinator.openIncoming(offer(fileSize = 1L), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))

        assertThrows<CheckpointConflictException> {
            val lastHash = ByteArray(ChunkCodec.DIGEST_SIZE)
            coordinator.commitChunk(
                session,
                1L,
                1,
                DigestChain.next(session.checkpoint.chainDigest, 0, 1, lastHash),
                lastHash
            )
        }
        Unit
    }

    @Test
    fun `success deletes only the relevant local checkpoint or link`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = coordinator(store, files)
        val prepared = coordinator.prepareOutgoing(source(), "peer-1", "sender-1")
        val incoming = coordinator.openIncoming(offer(), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))

        assertEquals("fake://${incoming.handle.location.value}", coordinator.completeIncoming(incoming))
        assertNull(store.findIncoming(incoming.checkpoint.transferId))
        assertTrue(store.outgoing.isNotEmpty())

        coordinator.completeOutgoing(prepared.offer.transferId)
        assertTrue(store.outgoing.isEmpty())
    }

    @Test
    fun `process restart removes deterministic staging created before checkpoint CAS`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles().apply { crashAfterCreate = true }
        val offer = offer()

        assertThrows<SimulatedProcessDeath> {
            coordinator(store, files).openIncoming(
                offer,
                TransferStartMode.NEW,
                ResumeStatus(ResumeState.NONE)
            )
        }

        assertEquals(1, store.stagingJournals.size)
        assertEquals(1, files.stagingCount())

        files.crashAfterCreate = false
        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)

        assertTrue(store.stagingJournals.isEmpty())
        assertEquals(0, files.stagingCount())
        assertNull(store.findIncoming(offer.transferId))
    }

    @Test
    fun `process restart keeps switched staging and recovers its retired file`() = runBlocking {
        val store = FakeResumeStore().apply { crashAfterRestartReplace = true }
        val files = FakeFiles()
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))

        assertThrows<SimulatedProcessDeath> {
            coordinator(store, files).openIncoming(
                offer,
                TransferStartMode.RESTART,
                coordinator(store, files).queryIncoming(offer)
            )
        }

        val switched = store.findIncoming(offer.transferId)!!
        assertEquals(old.value, switched.retiredStorageValue)
        assertTrue(files.contains(old))

        store.crashAfterRestartReplace = false
        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)

        val recovered = store.findIncoming(offer.transferId)!!
        assertNull(recovered.retiredStorageValue)
        assertEquals(IncomingOperationState.IDLE, recovered.operationState)
        assertFalse(files.contains(old))
        assertTrue(files.contains(StoredFileLocation(recovered.storageKind, recovered.storageValue)))
        assertTrue(store.stagingJournals.isEmpty())
    }

    @Test
    fun `startup keeps switched journal until retired deletion succeeds even when staging lookup misses`() = runBlocking {
        val store = FakeResumeStore().apply { crashAfterRestartReplace = true }
        val files = FakeFiles()
        val offer = offer()
        val old = files.put(offer.transferId, byteArrayOf(1))
        store.saveIncoming(checkpoint(offer, old))

        assertThrows<SimulatedProcessDeath> {
            coordinator(store, files).openIncoming(
                offer,
                TransferStartMode.RESTART,
                coordinator(store, files).queryIncoming(offer)
            )
        }
        val replacement = requireNotNull(store.findIncoming(offer.transferId))
        files.forgetStaging(StoredFileLocation(replacement.storageKind, replacement.storageValue))
        files.failDeleteValues += old.value
        store.crashAfterRestartReplace = false

        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)

        assertEquals(1, store.stagingJournals.size)
        assertEquals(old.value, store.findIncoming(offer.transferId)?.retiredStorageValue)

        files.failDeleteValues.clear()
        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)
        assertTrue(store.stagingJournals.isEmpty())
        assertNull(store.findIncoming(offer.transferId)?.retiredStorageValue)
    }

    @Test
    fun `heartbeat prevents stale recovery from clearing an active session`() = runBlocking {
        var time = now
        val store = FakeResumeStore()
        val files = FakeFiles()
        val coordinator = ResumeCoordinator(store, files, clock = { time })
        val session = coordinator.openIncoming(
            offer(), TransferStartMode.NEW, ResumeStatus(ResumeState.NONE)
        )

        time += 10_000
        val refreshed = coordinator.heartbeatIncoming(session)
        val cleared = store.clearStaleIncomingSessions(time - 1)

        assertEquals(0, cleared)
        assertEquals(time, store.findIncoming(session.checkpoint.transferId)?.sessionClaimedAt)
        assertEquals(IncomingOperationState.ACTIVE, refreshed.checkpoint.operationState)
    }

    @Test
    fun `completion enters durable state before stale clearing and process restart finishes publish`() = runBlocking {
        val store = FakeResumeStore().apply { clearStaleDuringBeginCompletion = true }
        val files = FakeFiles().apply { crashAfterPublish = true }
        val offer = offer()
        val session = coordinator(store, files).openIncoming(
            offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE)
        )

        assertThrows<SimulatedProcessDeath> {
            coordinator(store, files).completeIncoming(session)
        }

        val completing = store.findIncoming(offer.transferId)!!
        assertEquals(IncomingOperationState.COMPLETING, completing.operationState)
        assertEquals(0, store.clearedDuringCompletion)
        assertTrue(files.wasPublished(completing.location.toStoredLocationForTest()))

        files.crashAfterPublish = false
        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)

        assertNull(store.findIncoming(offer.transferId))
        assertEquals(1, files.publishCount)
    }

    @Test
    fun `completion persists receipt before checkpoint removal and matching query is completed`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles()
        val offer = offer(fileSize = 0)
        val coordinator = coordinator(store, files)
        val session = coordinator.openIncoming(offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        val digest = ChunkCodec.sha256(ByteArray(0))

        val published = coordinator.completeIncoming(session, digest)

        assertEquals("fake://${session.handle.location.value}", published)
        assertNull(store.findIncoming(offer.transferId))
        assertArrayEquals(digest, store.receipts.getValue(offer.transferId).finalDigest)
        val status = coordinator.queryIncoming(offer)
        assertEquals(ResumeState.COMPLETED, status.state)
        assertArrayEquals(digest, status.finalDigest)
    }

    @Test
    fun `receipt with different transfer identity is invalid`() = runBlocking {
        val store = FakeResumeStore()
        val offer = offer(fileSize = 0)
        val coordinator = coordinator(store, FakeFiles())
        val session = coordinator.openIncoming(offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        coordinator.completeIncoming(session, ChunkCodec.sha256(ByteArray(0)))

        val status = coordinator.queryIncoming(offer.copy(fileName = "different.bin"))

        assertEquals(ResumeState.INVALID, status.state)
    }

    @Test
    fun `restart after publish before receipt recovers one publish and completed receipt`() = runBlocking {
        val store = FakeResumeStore().apply { failReceiptCompletionOnce = true }
        val files = FakeFiles()
        val offer = offer(fileSize = 0)
        val first = coordinator(store, files)
        val session = first.openIncoming(offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        val digest = ChunkCodec.sha256(ByteArray(0))

        assertThrows<IllegalStateException> { first.completeIncoming(session, digest) }
        assertEquals(IncomingOperationState.COMPLETING, store.findIncoming(offer.transferId)?.operationState)

        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)

        assertEquals(1, files.publishCount)
        assertNull(store.findIncoming(offer.transferId))
        assertArrayEquals(digest, store.receipts.getValue(offer.transferId).finalDigest)
        assertEquals(ResumeState.COMPLETED, coordinator(store, files).queryIncoming(offer).state)
    }

    @Test
    fun `restart after crash before publish uses persisted digest and publishes once`() = runBlocking {
        val store = FakeResumeStore()
        val files = FakeFiles().apply { failBeforePublishOnce = true }
        val offer = offer(fileSize = 0)
        val first = coordinator(store, files)
        val session = first.openIncoming(offer, TransferStartMode.NEW, ResumeStatus(ResumeState.NONE))
        val digest = ChunkCodec.sha256(ByteArray(0))

        assertThrows<IllegalStateException> { first.completeIncoming(session, digest) }
        val completing = store.findIncoming(offer.transferId)!!
        assertArrayEquals(digest, completing.completingFinalDigest)
        assertEquals(0, files.publishCount)

        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)

        assertEquals(1, files.publishCount)
        assertArrayEquals(digest, store.receipts.getValue(offer.transferId).finalDigest)
        assertNull(store.findIncoming(offer.transferId))
    }

    @Test
    fun `v3 completing pending file derives persists digest then publishes once`() = runBlocking {
        val bytes = byteArrayOf(3, 4, 5)
        val offer = offer(bytes.size.toLong())
        val store = FakeResumeStore()
        val files = FakeFiles()
        val location = files.put(offer.transferId, bytes)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(bytes), bytes.size.toLong())
        store.saveIncoming(
            checkpoint(offer, location, prefix).copy(
                operationState = IncomingOperationState.COMPLETING,
                completingFinalDigest = null
            )
        )

        assertEquals(1, coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE))

        assertEquals(1, files.publishCount)
        assertArrayEquals(ChunkCodec.sha256(bytes), store.receipts.getValue(offer.transferId).finalDigest)
        assertTrue(store.persistedCompletingDigests.contains(offer.transferId))
    }

    @Test
    fun `v3 completing already published file derives digest without republishing`() = runBlocking {
        val bytes = byteArrayOf(7, 8)
        val offer = offer(bytes.size.toLong())
        val store = FakeResumeStore()
        val files = FakeFiles()
        val location = files.put(offer.transferId, bytes)
        val handle = requireNotNull(files.reopen(location, offer.fileName))
        files.publish(handle)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(bytes), bytes.size.toLong())
        store.saveIncoming(
            checkpoint(offer, location, prefix).copy(
                operationState = IncomingOperationState.COMPLETING,
                completingFinalDigest = null
            )
        )

        coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE)

        assertEquals(1, files.publishCount)
        assertArrayEquals(ChunkCodec.sha256(bytes), store.receipts.getValue(offer.transferId).finalDigest)
    }

    @Test
    fun `missing legacy completing file remains retryable and later recovery succeeds`() = runBlocking {
        val bytes = byteArrayOf(9)
        val offer = offer(bytes.size.toLong())
        val store = FakeResumeStore()
        val files = FakeFiles()
        val location = StoredFileLocation("FAKE", offer.transferId)
        val prefix = PrefixDigestScanner.scan(ByteArrayInputStream(bytes), bytes.size.toLong())
        store.saveIncoming(
            checkpoint(offer, location, prefix).copy(
                operationState = IncomingOperationState.COMPLETING,
                completingFinalDigest = null
            )
        )

        assertEquals(0, coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE))
        assertEquals(IncomingOperationState.COMPLETING, store.findIncoming(offer.transferId)?.operationState)

        files.put(offer.transferId, bytes)
        assertEquals(1, coordinator(store, files).recoverInterruptedState(Long.MAX_VALUE))
        assertEquals(ResumeState.COMPLETED, coordinator(store, files).queryIncoming(offer).state)
    }

    @Test
    fun `receipt is completed before expiry and atomically becomes none at expiry`() = runBlocking {
        var time = now
        val offer = offer(fileSize = 0)
        val store = FakeResumeStore()
        store.receipts[offer.transferId] = CompletedReceipt(
            offer.transferId, offer.senderDeviceId, offer.fileName, offer.mimeType,
            offer.fileSize, offer.chunkSize, ChunkCodec.sha256(ByteArray(0)),
            "fake://done", offer.fileName, time, time + 10
        )
        val coordinator = ResumeCoordinator(store, FakeFiles(), clock = { time })

        assertEquals(ResumeState.COMPLETED, coordinator.queryIncoming(offer).state)
        time += 10
        assertEquals(ResumeState.NONE, coordinator.queryIncoming(offer).state)
        assertNull(store.receipts[offer.transferId])
    }

    private fun coordinator(
        store: FakeResumeStore = FakeResumeStore(),
        files: FakeFiles = FakeFiles()
    ) = ResumeCoordinator(store, files, clock = { now }, newTransferId = { UUID.randomUUID().toString() })

    private fun source(
        bytes: ByteArray = byteArrayOf(1, 2, 3),
        length: Long = bytes.size.toLong(),
        lastModified: Long? = 42L,
        currentLength: Long = length,
        currentLastModified: Long? = lastModified
    ) = SendFileSource(
        displayName = "a.bin",
        mimeType = "application/octet-stream",
        length = length,
        sourceUri = "content://a",
        lastModified = lastModified,
        openStream = { ByteArrayInputStream(bytes) },
        metadataProvider = { SendFileMetadata(currentLength, currentLastModified) }
    )

    private fun offer(fileSize: Long = 3L) = TransferOffer(
        transferId = "11111111-1111-1111-1111-111111111111",
        senderDeviceId = "sender-1",
        fileName = "a.bin",
        mimeType = "application/octet-stream",
        fileSize = fileSize
    )

    private fun checkpoint(
        offer: TransferOffer,
        location: StoredFileLocation,
        prefix: com.example.transfer.protocol.PrefixDigest = PrefixDigestScanner.scan(
            ByteArrayInputStream(ByteArray(0)),
            0
        )
    ) = IncomingCheckpoint(
        transferId = offer.transferId,
        senderDeviceId = offer.senderDeviceId,
        fileName = offer.fileName,
        displayName = offer.fileName,
        mimeType = offer.mimeType,
        fileSize = offer.fileSize,
        chunkSize = offer.chunkSize,
        confirmedBytes = prefix.scannedBytes,
        nextChunkIndex = prefix.nextChunkIndex,
        chainDigest = prefix.chainDigest,
        lastChunkHash = prefix.lastChunkHash,
        storageKind = location.kind,
        storageValue = location.value,
        createdAt = now - 1,
        updatedAt = now - 1,
        expiresAt = now + ResumeCoordinator.RETENTION_MILLIS
    )
}

private class SimulatedProcessDeath : Error("simulated process death")

private fun ResumeStorageLocation.toStoredLocationForTest() = StoredFileLocation(kind, value)

internal inline fun <reified T : Throwable> assertThrows(block: () -> Unit): T {
    try {
        block()
    } catch (error: Throwable) {
        if (error is T) return error
        throw error
    }
    fail("Expected ${T::class.java.simpleName}")
    throw AssertionError("unreachable")
}

private class FakeResumeStore : ResumeStore {
    val incoming = linkedMapOf<String, IncomingCheckpoint>()
    val outgoing = linkedMapOf<String, OutgoingResumeLink>()
    val receipts = linkedMapOf<String, CompletedReceipt>()
    val persistedCompletingDigests = mutableListOf<String>()
    val deletedOutgoing = mutableListOf<String>()
    val expiryUpdates = mutableListOf<String>()
    var allowCommit = true
    var allowInsert = true
    var allowReplace = true
    var allowRelease = true
    var allowJournalInsert = true
    var journalInsertFailure: Throwable? = null
    var journalDeleteFailure: Throwable? = null
    var releaseFailureAfterClear: Throwable? = null
    var allowExpiryUpdate = true
    var removeAfterExpiryUpdate = false
    var advanceAfterExpiryUpdate = false
    var forceObserved: (() -> Boolean)? = null
    val stagingJournals = linkedMapOf<String, IncomingStagingJournal>()
    var crashAfterRestartReplace = false
    var clearStaleDuringBeginCompletion = false
    var clearedDuringCompletion = -1
    var failReceiptCompletionOnce = false

    override suspend fun findIncoming(transferId: String) = incoming[transferId]
    override suspend fun saveIncoming(checkpoint: IncomingCheckpoint) { incoming[checkpoint.transferId] = checkpoint }
    override suspend fun insertIncoming(checkpoint: IncomingCheckpoint): Boolean {
        if (!allowInsert) return false
        if (incoming.containsKey(checkpoint.transferId)) return false
        incoming[checkpoint.transferId] = checkpoint
        return true
    }
    override suspend fun acquireIncomingSession(
        expected: IncomingCheckpoint, token: String, now: Long, expiresAt: Long
    ): IncomingCheckpoint? {
        expiryUpdates += expected.transferId
        val current = incoming[expected.transferId]
        if (!allowExpiryUpdate || current == null || current.sessionToken != null ||
            current.operationState != IncomingOperationState.IDLE ||
            current.generation != expected.generation || current.storageValue != expected.storageValue ||
            current.nextChunkIndex != expected.nextChunkIndex ||
            !current.chainDigest.contentEquals(expected.chainDigest)
        ) return null
        incoming[expected.transferId] = current.copy(
            sessionToken = token, sessionClaimedAt = now,
            operationState = IncomingOperationState.ACTIVE,
            updatedAt = now, expiresAt = expiresAt
        )
        if (removeAfterExpiryUpdate) incoming.remove(expected.transferId)
        if (advanceAfterExpiryUpdate) {
            incoming[expected.transferId] = incoming.getValue(expected.transferId).copy(
                confirmedBytes = current.fileSize, nextChunkIndex = 1,
                chainDigest = ByteArray(ChunkCodec.DIGEST_SIZE) { 3 },
                lastChunkHash = ByteArray(ChunkCodec.DIGEST_SIZE) { 4 }
            )
        }
        return incoming[expected.transferId]
    }
    override suspend fun releaseIncomingSession(expected: IncomingCheckpoint, token: String): Boolean {
        if (!allowRelease) return false
        val current = incoming[expected.transferId] ?: return false
        if (current.sessionToken != token || current.generation != expected.generation ||
            current.storageValue != expected.storageValue || current.nextChunkIndex != expected.nextChunkIndex
        ) return false
        if (current.operationState != IncomingOperationState.ACTIVE) return false
        incoming[expected.transferId] = current.copy(
            sessionToken = null,
            sessionClaimedAt = null,
            operationState = IncomingOperationState.IDLE
        )
        releaseFailureAfterClear?.let { throw it }
        return true
    }
    override suspend fun clearStaleIncomingSessions(staleClaimBefore: Long): Int {
        val stale = incoming.values.filter {
            it.operationState == IncomingOperationState.ACTIVE && it.sessionToken != null &&
                (it.sessionClaimedAt ?: Long.MAX_VALUE) < staleClaimBefore
        }
        stale.forEach {
            incoming[it.transferId] = it.copy(
                sessionToken = null,
                sessionClaimedAt = null,
                operationState = IncomingOperationState.IDLE
            )
        }
        return stale.size
    }
    override suspend fun replaceIncomingForRestart(
        expected: IncomingCheckpoint, replacement: IncomingCheckpoint, token: String
    ): Boolean {
        if (!allowReplace) return false
        val current = incoming[expected.transferId] ?: return false
        if (current.sessionToken != token || current.generation != expected.generation ||
            current.storageValue != expected.storageValue || current.nextChunkIndex != expected.nextChunkIndex
        ) return false
        incoming[expected.transferId] = replacement
        if (crashAfterRestartReplace) throw SimulatedProcessDeath()
        return true
    }
    override suspend fun clearRetiredIncoming(expected: IncomingCheckpoint, token: String): Boolean {
        val current = incoming[expected.transferId] ?: return false
        if (current.sessionToken != token || current.generation != expected.generation ||
            current.storageValue != expected.storageValue || current.nextChunkIndex != expected.nextChunkIndex
        ) return false
        incoming[expected.transferId] = current.copy(retiredStorageKind = null, retiredStorageValue = null)
        return true
    }
    override suspend fun heartbeatIncomingSession(
        expected: IncomingCheckpoint, token: String, now: Long, expiresAt: Long
    ): Boolean {
        val current = incoming[expected.transferId] ?: return false
        if (current.sessionToken != token || current.generation != expected.generation ||
            current.storageValue != expected.storageValue || current.nextChunkIndex != expected.nextChunkIndex ||
            current.operationState != IncomingOperationState.ACTIVE
        ) return false
        incoming[expected.transferId] = current.copy(
            sessionClaimedAt = now, updatedAt = now, expiresAt = expiresAt
        )
        return true
    }
    override suspend fun beginIncomingCompletion(
        expected: IncomingCheckpoint, token: String, finalDigest: ByteArray,
        now: Long, expiresAt: Long
    ): Boolean {
        val current = incoming[expected.transferId] ?: return false
        if (current.sessionToken != token || current.generation != expected.generation ||
            current.storageValue != expected.storageValue || current.nextChunkIndex != expected.nextChunkIndex ||
            current.operationState != IncomingOperationState.ACTIVE
        ) return false
        incoming[expected.transferId] = current.copy(
            operationState = IncomingOperationState.COMPLETING,
            sessionClaimedAt = now, updatedAt = now, expiresAt = expiresAt,
            completingFinalDigest = finalDigest.copyOf()
        )
        if (clearStaleDuringBeginCompletion) {
            clearedDuringCompletion = clearStaleIncomingSessions(Long.MAX_VALUE)
        }
        return true
    }
    override suspend fun findCompletingIncoming(): List<IncomingCheckpoint> =
        incoming.values.filter { it.operationState == IncomingOperationState.COMPLETING }
    override suspend fun persistRecoveredCompletingDigest(
        expected: IncomingCheckpoint,
        finalDigest: ByteArray,
        now: Long,
        expiresAt: Long
    ): IncomingCheckpoint? {
        val current = incoming[expected.transferId] ?: return null
        if (current.operationState != IncomingOperationState.COMPLETING ||
            current.completingFinalDigest != null || current.confirmedBytes != current.fileSize ||
            current.generation != expected.generation || current.storageValue != expected.storageValue
        ) return null
        val updated = current.copy(
            completingFinalDigest = finalDigest.copyOf(), updatedAt = now, expiresAt = expiresAt
        )
        incoming[expected.transferId] = updated
        persistedCompletingDigests += expected.transferId
        return updated
    }
    override suspend fun deleteCompletingIncoming(expected: IncomingCheckpoint): Boolean {
        val current = incoming[expected.transferId] ?: return false
        if (current.operationState != IncomingOperationState.COMPLETING ||
            current.generation != expected.generation || current.storageValue != expected.storageValue
        ) return false
        incoming.remove(expected.transferId)
        return true
    }
    override suspend fun findCompletedReceipt(transferId: String, now: Long): CompletedReceipt? {
        val receipt = receipts[transferId] ?: return null
        if (receipt.expiresAt <= now) {
            receipts.remove(transferId)
            return null
        }
        return receipt
    }
    override suspend fun finishIncomingCompletion(
        expected: IncomingCheckpoint,
        receipt: CompletedReceipt
    ): Boolean {
        if (failReceiptCompletionOnce) {
            failReceiptCompletionOnce = false
            throw IllegalStateException("receipt transaction failed")
        }
        val current = incoming[expected.transferId] ?: return false
        if (current.operationState != IncomingOperationState.COMPLETING ||
            current.generation != expected.generation || current.storageValue != expected.storageValue ||
            current.completingFinalDigest?.contentEquals(receipt.finalDigest) != true
        ) return false
        receipts[receipt.transferId] = receipt
        incoming.remove(expected.transferId)
        return true
    }
    override suspend fun clearRetiredIncomingForRecovery(expected: IncomingCheckpoint): Boolean {
        val current = incoming[expected.transferId] ?: return false
        if (current.generation != expected.generation || current.storageValue != expected.storageValue ||
            current.retiredStorageValue != expected.retiredStorageValue
        ) return false
        incoming[expected.transferId] = current.copy(
            retiredStorageKind = null, retiredStorageValue = null
        )
        return true
    }
    override suspend fun findIncomingWithRetiredStorage(): List<IncomingCheckpoint> =
        incoming.values.filter { it.retiredLocation != null }
    override suspend fun insertStagingJournal(journal: IncomingStagingJournal): Boolean {
        journalInsertFailure?.let { throw it }
        if (!allowJournalInsert) return false
        if (stagingJournals.containsKey(journal.transferId)) return false
        stagingJournals[journal.transferId] = journal
        return true
    }
    override suspend fun findStagingJournals(): List<IncomingStagingJournal> =
        stagingJournals.values.toList()
    override suspend fun deleteStagingJournal(journal: IncomingStagingJournal): Boolean {
        journalDeleteFailure?.let { throw it }
        return stagingJournals.remove(journal.transferId, journal)
    }
    override suspend fun deleteOwnedIncoming(expected: IncomingCheckpoint, token: String): Boolean {
        val current = incoming[expected.transferId] ?: return false
        if (current.sessionToken != token || current.generation != expected.generation ||
            current.storageValue != expected.storageValue || current.nextChunkIndex != expected.nextChunkIndex
        ) return false
        incoming.remove(expected.transferId)
        return true
    }
    override suspend fun commitIncomingChunk(
        transferId: String, expectedNextChunkIndex: Int, confirmedBytes: Long,
        nextChunkIndex: Int, chainDigest: ByteArray, lastChunkHash: ByteArray,
        updatedAt: Long, expiresAt: Long
    ): Boolean {
        check(forceObserved?.invoke() != false) { "commit happened before force" }
        val current = incoming[transferId]
        if (!allowCommit || current == null || current.nextChunkIndex != expectedNextChunkIndex) return false
        incoming[transferId] = current.copy(
            confirmedBytes = confirmedBytes, nextChunkIndex = nextChunkIndex,
            chainDigest = chainDigest, lastChunkHash = lastChunkHash,
            updatedAt = updatedAt, expiresAt = expiresAt
        )
        return true
    }
    override suspend fun commitOwnedIncomingChunk(
        expected: IncomingCheckpoint, token: String, confirmedBytes: Long,
        nextChunkIndex: Int, chainDigest: ByteArray, lastChunkHash: ByteArray,
        updatedAt: Long, expiresAt: Long
    ): Boolean {
        check(forceObserved?.invoke() != false) { "commit happened before force" }
        val current = incoming[expected.transferId]
        if (!allowCommit || current == null || current.sessionToken != token ||
            current.generation != expected.generation || current.storageValue != expected.storageValue ||
            current.nextChunkIndex != expected.nextChunkIndex
        ) return false
        incoming[expected.transferId] = current.copy(
            confirmedBytes = confirmedBytes, nextChunkIndex = nextChunkIndex,
            chainDigest = chainDigest, lastChunkHash = lastChunkHash,
            updatedAt = updatedAt, expiresAt = expiresAt
        )
        return true
    }
    override suspend fun updateIncomingExpiry(transferId: String, updatedAt: Long, expiresAt: Long): Boolean {
        expiryUpdates += transferId
        val current = incoming[transferId]
        if (!allowExpiryUpdate || current == null) return false
        incoming[transferId] = current.copy(updatedAt = updatedAt, expiresAt = expiresAt)
        if (removeAfterExpiryUpdate) incoming.remove(transferId)
        if (advanceAfterExpiryUpdate) {
            incoming[transferId] = incoming.getValue(transferId).copy(
                confirmedBytes = incoming.getValue(transferId).fileSize,
                nextChunkIndex = 1,
                chainDigest = ByteArray(ChunkCodec.DIGEST_SIZE) { 3 },
                lastChunkHash = ByteArray(ChunkCodec.DIGEST_SIZE) { 4 }
            )
        }
        return true
    }
    override suspend fun deleteIncoming(transferId: String) { incoming.remove(transferId) }
    override suspend fun findOutgoing(sourceUri: String, peerDeviceId: String) =
        outgoing.values.singleOrNull { it.sourceUri == sourceUri && it.peerDeviceId == peerDeviceId }
    override suspend fun saveOutgoing(link: OutgoingResumeLink) { outgoing[link.transferId] = link }
    override suspend fun resolveOutgoing(candidate: OutgoingResumeLink): OutgoingResumeLink = synchronized(this) {
            val existing = outgoing.values.singleOrNull {
                it.sourceUri == candidate.sourceUri && it.peerDeviceId == candidate.peerDeviceId
            }
            val resolved = if (existing != null && existing.fileName == candidate.fileName &&
                existing.mimeType == candidate.mimeType && existing.fileSize == candidate.fileSize &&
                existing.lastModified == candidate.lastModified && existing.chunkSize == candidate.chunkSize
            ) existing.copy(updatedAt = candidate.updatedAt) else candidate
            if (existing != null && existing.transferId != resolved.transferId) {
                deletedOutgoing += existing.transferId
                outgoing.remove(existing.transferId)
            }
            outgoing[resolved.transferId] = resolved
            resolved
        }
    override suspend fun updateOutgoingTimestamp(transferId: String, updatedAt: Long): Boolean {
        val link = outgoing[transferId] ?: return false
        outgoing[transferId] = link.copy(updatedAt = updatedAt)
        return true
    }
    override suspend fun deleteOutgoing(transferId: String) { deletedOutgoing += transferId; outgoing.remove(transferId) }
    override suspend fun deleteCompleted(transferId: String) { incoming.remove(transferId); outgoing.remove(transferId) }
    override suspend fun claimExpiredIncoming(now: Long, staleClaimBefore: Long, token: String) = emptyList<IncomingCheckpoint>()
    override suspend fun deleteClaimedIncoming(token: String) = 0
    override suspend fun releaseClaimedIncoming(token: String) = 0
    override suspend fun deleteExpiredOutgoing(updatedAtCutoff: Long) = 0
    override suspend fun deleteExpiredCompletedReceipts(now: Long): Int {
        val expired = receipts.values.filter { it.expiresAt <= now }.map { it.transferId }
        expired.forEach(receipts::remove)
        return expired.size
    }
}

private class FakeFiles : ResumableIncomingFileStore {
    private val entries = linkedMapOf<StoredFileLocation, FakeEntry>()
    private val staging = linkedMapOf<String, StoredFileLocation>()
    private val published = mutableSetOf<StoredFileLocation>()
    private val publishedEntries = linkedMapOf<StoredFileLocation, FakeEntry>()
    val deleted = mutableListOf<StoredFileLocation>()
    val createdHandles = mutableListOf<FakeHandle>()
    var reopenCount = 0
    var failCreate = false
    var crashAfterCreate = false
    var failAfterCreate = false
    var crashAfterPublish = false
    var failBeforePublishOnce = false
    var publishCount = 0
    var openInputFailure: Exception? = null
    var failCreatedHandleClose = false
    val failDeleteValues = mutableSetOf<String>()
    val cancelDeleteValues = mutableSetOf<String>()

    fun contains(location: StoredFileLocation) = entries.containsKey(location)
    fun stagingCount() = staging.size
    fun forgetStaging(location: StoredFileLocation) {
        staging.entries.removeAll { it.value == location }
    }
    fun wasPublished(location: StoredFileLocation) = location in published

    fun put(id: String, bytes: ByteArray): StoredFileLocation {
        val location = StoredFileLocation("FAKE", id)
        entries[location] = FakeEntry("a.bin", bytes)
        return location
    }

    fun replace(location: StoredFileLocation, bytes: ByteArray) {
        entries.getValue(location).bytes = bytes
    }

    override suspend fun create(transferId: String, fileName: String, mimeType: String): ResumableFileHandle {
        if (failCreate) throw IllegalStateException("create failed")
        check(transferId !in staging) { "staging already exists" }
        val location = StoredFileLocation("FAKE", transferId)
        val entry = FakeEntry(fileName, ByteArray(0))
        entries[location] = entry
        staging[transferId] = location
        if (crashAfterCreate) throw SimulatedProcessDeath()
        if (failAfterCreate) throw IllegalStateException("create partially failed")
        return FakeHandle(location, entry).also {
            it.failClose = failCreatedHandleClose
            createdHandles += it
        }
    }
    override suspend fun findStaging(transferId: String): StoredFileLocation? = staging[transferId]
    override suspend fun reopen(location: StoredFileLocation, displayName: String): ResumableFileHandle? {
        reopenCount++
        return entries[location]?.let { FakeHandle(location, it) }
    }
    override suspend fun openInput(location: StoredFileLocation): InputStream? =
        openInputFailure?.let { throw it } ?: entries[location]?.let { ByteArrayInputStream(it.bytes) }
    override suspend fun openCompletionInput(
        location: StoredFileLocation,
        displayName: String
    ): InputStream? = openInputFailure?.let { throw it } ?:
        (entries[location] ?: publishedEntries[location])?.let { ByteArrayInputStream(it.bytes) }
    override suspend fun publish(
        handle: ResumableFileHandle,
        expectedDigest: ByteArray?
    ): String {
        if (failBeforePublishOnce) {
            failBeforePublishOnce = false
            throw IllegalStateException("publish failed before effect")
        }
        publishCount++
        handle.close()
        entries.remove(handle.location)?.let { publishedEntries[handle.location] = it.copy(bytes = it.bytes.copyOf()) }
        staging.entries.removeAll { it.value == handle.location }
        published += handle.location
        if (crashAfterPublish) throw SimulatedProcessDeath()
        return "fake://${handle.location.value}"
    }
    override suspend fun recoverPublished(
        location: StoredFileLocation,
        displayName: String,
        expectedDigest: ByteArray?
    ): String? =
        if (location in published) "fake://${location.value}" else null
    override suspend fun delete(location: StoredFileLocation) {
        if (location.value in cancelDeleteValues) throw CancellationException("delete cancelled")
        if (location.value in failDeleteValues) throw IllegalStateException("delete failed")
        deleted += location
        entries.remove(location)
        staging.entries.removeAll { it.value == location }
    }
}

private data class FakeEntry(val name: String, var bytes: ByteArray)

private class FakeHandle(
    override val location: StoredFileLocation,
    private val entry: FakeEntry
) : ResumableFileHandle {
    override val displayName: String get() = entry.name
    var forced = false
    var failClose = false
    var isClosed = false
        private set
    override fun length() = entry.bytes.size.toLong()
    override fun writeAt(offset: Long, source: ByteArray, length: Int) {
        val end = offset.toInt() + length
        if (end > entry.bytes.size) entry.bytes = entry.bytes.copyOf(end)
        source.copyInto(entry.bytes, offset.toInt(), 0, length)
    }
    override fun truncate(length: Long) { entry.bytes = entry.bytes.copyOf(length.toInt()) }
    override fun force() { forced = true }
    override fun close() {
        isClosed = true
        if (failClose) throw IllegalStateException("close failed")
    }
}
