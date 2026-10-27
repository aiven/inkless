/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlPlaneAvailabilityTest {
    @Test
    void startsUnknownAndReadsAsUnavailable() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            assertEquals(ControlPlaneAvailability.State.UNKNOWN, availability.state());
            assertFalse(availability.isAvailable(),
                "an untried control plane must not let produce buffer and upload before the "
                    + "first attempt settles");
            assertNull(availability.unavailableReason());
        }
    }

    @Test
    void markUnavailableBlocksAndRecordsTheReason() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            assertEquals(ControlPlaneAvailability.State.UNAVAILABLE, availability.state());
            assertFalse(availability.isAvailable());
            assertEquals(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED, availability.unavailableReason());
        }
    }

    @Test
    void markAvailableUnblocksAndClearsTheReason() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            availability.markAvailable();
            assertEquals(ControlPlaneAvailability.State.AVAILABLE, availability.state());
            assertTrue(availability.isAvailable());
            assertNull(availability.unavailableReason());
        }
    }

    @Test
    void markUnknownResetsToUntriedAndClearsTheReason() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            availability.markUnknown();
            assertEquals(ControlPlaneAvailability.State.UNKNOWN, availability.state());
            assertFalse(availability.isAvailable());
            assertNull(availability.unavailableReason());
        }
    }

    @Test
    void recordGatedCallDoesNotChangeState() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            availability.recordGatedCall();
            assertEquals(ControlPlaneAvailability.State.UNAVAILABLE, availability.state());
        }
    }

    @Test
    void recordGatedCallBeforeAnyFailureDoesNotChangeState() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.recordGatedCall();
            assertEquals(ControlPlaneAvailability.State.UNKNOWN, availability.state());
            // unavailableReason() remains null. NOT_READY is only how the gated call gets counted,
            // it is never a value unavailableReason() reports while UNKNOWN.
            assertNull(availability.unavailableReason());
        }
    }

    @RepeatedTest(3)
    void recordGatedCallSurvivesConcurrentReasonClears() throws Exception {
        final int iterations = 200_000;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean writerDone = new AtomicBoolean();
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            final CountDownLatch start = new CountDownLatch(1);
            final Future<?> writer = executor.submit(() -> {
                start.await();
                try {
                    for (int i = 0; i < iterations && failure.get() == null; i++) {
                        availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
                        availability.markAvailable();
                    }
                } finally {
                    writerDone.set(true);
                }
                return null;
            });
            final Future<?> reader = executor.submit(() -> {
                start.await();
                try {
                    while (!writerDone.get() && failure.get() == null) {
                        availability.recordGatedCall();
                    }
                } catch (final Throwable t) {
                    failure.set(t);
                }
                return null;
            });
            start.countDown();
            writer.get(30, TimeUnit.SECONDS);
            reader.get(30, TimeUnit.SECONDS);
            assertNull(failure.get(), "recordGatedCall must read the volatile reason once");
        } finally {
            executor.shutdownNow();
        }
    }
}
