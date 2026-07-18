package com.example.transfer.service

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class TransferLaneGateTest {
    @Test
    fun `incoming and outgoing hold independent resource leases`() {
        var leases = 0
        val gate = TransferLaneGate(
            acquireResources = { leases++ },
            releaseResources = { leases-- }
        )

        assertTrue(gate.begin(TransferLane.OUTGOING))
        assertTrue(gate.begin(TransferLane.INCOMING))

        assertTrue(gate.isActive(TransferLane.OUTGOING))
        assertTrue(gate.isActive(TransferLane.INCOMING))
        assertEquals(2, leases)
    }

    @Test
    fun `second begin in an active direction is rejected without another lease`() {
        var leases = 0
        val gate = TransferLaneGate(
            acquireResources = { leases++ },
            releaseResources = { leases-- }
        )

        assertTrue(gate.begin(TransferLane.OUTGOING))
        assertFalse(gate.begin(TransferLane.OUTGOING))
        assertEquals(1, leases)

        assertTrue(gate.begin(TransferLane.INCOMING))
        assertFalse(gate.begin(TransferLane.INCOMING))
        assertEquals(2, leases)
    }

    @Test
    fun `ending one direction leaves the other direction active`() {
        var leases = 0
        val gate = TransferLaneGate(
            acquireResources = { leases++ },
            releaseResources = { leases-- }
        )
        assertTrue(gate.begin(TransferLane.OUTGOING))
        assertTrue(gate.begin(TransferLane.INCOMING))

        assertTrue(gate.end(TransferLane.OUTGOING))

        assertFalse(gate.isActive(TransferLane.OUTGOING))
        assertTrue(gate.isActive(TransferLane.INCOMING))
        assertEquals(1, leases)
        assertFalse(gate.end(TransferLane.OUTGOING))
        assertEquals(1, leases)

        assertTrue(gate.end(TransferLane.INCOMING))
        assertFalse(gate.isActive(TransferLane.OUTGOING))
        assertFalse(gate.isActive(TransferLane.INCOMING))
        assertEquals(0, leases)
    }

    @Test
    fun `failed resource acquisition rolls the lane back to inactive`() {
        var attempts = 0
        val gate = TransferLaneGate(
            acquireResources = {
                attempts++
                throw IllegalStateException("resource unavailable")
            },
            releaseResources = {}
        )

        assertFalse(gate.begin(TransferLane.OUTGOING))

        assertFalse(gate.isActive(TransferLane.OUTGOING))
        assertEquals(1, attempts)
    }
}
