package com.example.transfer.service

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class TransferServiceLifecycleCoordinatorTest {
    @Test
    fun `incoming and outgoing are admitted together`() {
        val fixture = fixture()

        assertTrue(fixture.coordinator.beginOutgoing())
        assertTrue(fixture.coordinator.beginIncoming())

        assertEquals(2, fixture.leases)
    }

    @Test
    fun `second same direction admission is rejected`() {
        val fixture = fixture()

        assertTrue(fixture.coordinator.beginOutgoing())
        assertFalse(fixture.coordinator.beginOutgoing())
        assertTrue(fixture.coordinator.beginIncoming())
        assertFalse(fixture.coordinator.beginIncoming())

        assertEquals(2, fixture.leases)
    }

    @Test
    fun `ending outgoing leaves incoming active and one lease held`() {
        val fixture = fixture()
        fixture.coordinator.beginOutgoing()
        fixture.coordinator.beginIncoming()

        assertTrue(fixture.coordinator.endOutgoing())

        assertFalse(fixture.coordinator.isOutgoingActive())
        assertTrue(fixture.coordinator.isIncomingActive())
        assertEquals(1, fixture.leases)
    }

    @Test
    fun `drain with both lanes active releases two leases`() {
        val fixture = fixture()
        fixture.coordinator.beginOutgoing()
        fixture.coordinator.beginIncoming()

        fixture.coordinator.drain()

        assertFalse(fixture.coordinator.isOutgoingActive())
        assertFalse(fixture.coordinator.isIncomingActive())
        assertEquals(0, fixture.leases)
        assertEquals(2, fixture.releaseCalls)
    }

    @Test
    fun `normal cleanup followed by drain does not double release`() {
        val fixture = fixture()
        fixture.coordinator.beginOutgoing()
        fixture.coordinator.beginIncoming()

        fixture.coordinator.endOutgoing()
        fixture.coordinator.endIncoming()
        fixture.coordinator.drain()

        assertEquals(0, fixture.leases)
        assertEquals(2, fixture.releaseCalls)
    }

    @Test
    fun `drain followed by late normal cleanup does not double release`() {
        val fixture = fixture()
        fixture.coordinator.beginOutgoing()
        fixture.coordinator.beginIncoming()

        fixture.coordinator.drain()
        assertFalse(fixture.coordinator.endOutgoing())
        assertFalse(fixture.coordinator.endIncoming())

        assertEquals(0, fixture.leases)
        assertEquals(2, fixture.releaseCalls)
    }

    private fun fixture(): Fixture {
        lateinit var fixture: Fixture
        val gate = TransferLaneGate(
            acquireResources = { fixture.leases++ },
            releaseResources = {
                fixture.leases--
                fixture.releaseCalls++
            }
        )
        fixture = Fixture(TransferServiceLifecycleCoordinator(gate))
        return fixture
    }

    private data class Fixture(
        val coordinator: TransferServiceLifecycleCoordinator,
        var leases: Int = 0,
        var releaseCalls: Int = 0,
    )
}
