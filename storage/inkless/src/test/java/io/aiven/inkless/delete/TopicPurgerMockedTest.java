/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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
package io.aiven.inkless.delete;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.PurgeDeletedLogsResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TopicPurgerMockedTest {
    static final int MAX_BATCHES_PER_CYCLE = 3;
    Time time = new MockTime();

    @Mock
    ControlPlane controlPlane;

    @Test
    void empty() {
        final var purger = new TopicPurger(time, controlPlane, MAX_BATCHES_PER_CYCLE);
        when(controlPlane.purgeDeletedLogs(MAX_BATCHES_PER_CYCLE))
            .thenReturn(PurgeDeletedLogsResponse.empty());

        assertEquals(-1, purger.metrics.lastSuccessfulPurgeTimeMs.get());

        final long beforeMs = time.milliseconds();
        purger.run();

        verify(controlPlane, times(1)).purgeDeletedLogs(eq(MAX_BATCHES_PER_CYCLE));
        // A cycle with no work still counts: the gauge answers "is the purger running", not "is it deleting".
        assertEquals(time.milliseconds(), purger.metrics.lastSuccessfulPurgeTimeMs.get());
        assertEquals(0, purger.metrics.topicPurgerCycleSaturated.intValue());
        assertEquals(0, purger.metrics.topicPurgerWorkRemain.get());
        assertEquals(beforeMs, time.milliseconds());
    }

    @Test
    void noWorkDoesNotSleepWhenMoreRemain() {
        final var purger = new TopicPurger(time, controlPlane, MAX_BATCHES_PER_CYCLE);
        // Another broker holds the deleted rows (SKIP LOCKED miss): moreRemain is cluster-wide.
        when(controlPlane.purgeDeletedLogs(MAX_BATCHES_PER_CYCLE))
            .thenReturn(new PurgeDeletedLogsResponse(0, 0, 0, true, false));

        final long beforeMs = time.milliseconds();
        purger.run();

        assertEquals(beforeMs, time.milliseconds());
        assertEquals(0, purger.metrics.topicPurgerCycleSaturated.intValue());
        assertEquals(1, purger.metrics.topicPurgerWorkRemain.get());
        assertEquals(time.milliseconds(), purger.metrics.lastSuccessfulPurgeTimeMs.get());
    }

    @Test
    void workAndSaturation() {
        final var purger = new TopicPurger(time, controlPlane, MAX_BATCHES_PER_CYCLE);
        when(controlPlane.purgeDeletedLogs(MAX_BATCHES_PER_CYCLE))
            .thenReturn(new PurgeDeletedLogsResponse(3, 0, 1, true, true));

        purger.run();

        verify(controlPlane, times(1)).purgeDeletedLogs(eq(MAX_BATCHES_PER_CYCLE));
        assertEquals(1, purger.metrics.topicPurgerCycleSaturated.intValue());
        assertEquals(1, purger.metrics.topicPurgerWorkRemain.get());
        assertEquals(time.milliseconds(), purger.metrics.lastSuccessfulPurgeTimeMs.get());
    }

    @Test
    void emptyLogCapSaturates() {
        final var purger = new TopicPurger(time, controlPlane, MAX_BATCHES_PER_CYCLE);
        when(controlPlane.purgeDeletedLogs(MAX_BATCHES_PER_CYCLE))
            .thenReturn(new PurgeDeletedLogsResponse(0, MAX_BATCHES_PER_CYCLE, 0, true, true));

        purger.run();

        assertEquals(1, purger.metrics.topicPurgerCycleSaturated.intValue());
        assertEquals(1, purger.metrics.topicPurgerWorkRemain.get());
    }

    @Test
    void unboundedDoesNotSaturate() {
        final var purger = new TopicPurger(time, controlPlane, 0);
        when(controlPlane.purgeDeletedLogs(0))
            .thenReturn(new PurgeDeletedLogsResponse(10, 2, 3, false, false));

        purger.run();

        assertEquals(0, purger.metrics.topicPurgerCycleSaturated.intValue());
        assertEquals(time.milliseconds(), purger.metrics.lastSuccessfulPurgeTimeMs.get());
    }

    @Test
    void errorBackoffSkipsTicksUntilEligible() {
        final var purger = new TopicPurger(time, controlPlane, MAX_BATCHES_PER_CYCLE);
        when(controlPlane.purgeDeletedLogs(MAX_BATCHES_PER_CYCLE))
            .thenThrow(new RuntimeException("purge failed"))
            .thenReturn(PurgeDeletedLogsResponse.empty());

        purger.run();
        assertEquals(-1, purger.metrics.lastSuccessfulPurgeTimeMs.get());
        verify(controlPlane, times(1)).purgeDeletedLogs(eq(MAX_BATCHES_PER_CYCLE));

        purger.run();
        assertEquals(-1, purger.metrics.lastSuccessfulPurgeTimeMs.get());
        verify(controlPlane, times(1)).purgeDeletedLogs(eq(MAX_BATCHES_PER_CYCLE));

        // First error backoff is 100 * 2^1 * [0.8, 1.2] ms. Jump past the max.
        time.sleep(1_000);
        purger.run();
        assertEquals(time.milliseconds(), purger.metrics.lastSuccessfulPurgeTimeMs.get());
        verify(controlPlane, times(2)).purgeDeletedLogs(eq(MAX_BATCHES_PER_CYCLE));
    }
}
