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
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.function.Supplier;

import io.aiven.inkless.cache.ObjectCache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * The in-flight gauges are the only new metrics carrying arithmetic rather than a straight histogram
 * update, and every other test in this package mocks this class, so they would otherwise go unchecked.
 */
public class InklessFetchMetricsTest {
    private final Time time = new MockTime();

    private InklessFetchMetrics metrics;
    private Supplier<Long> inFlight;
    private Supplier<Long> inFlightMax;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setup() {
        final KafkaMetricsGroup metricsGroup = mock(KafkaMetricsGroup.class);
        metrics = new InklessFetchMetrics(time, mock(ObjectCache.class), metricsGroup);

        final ArgumentCaptor<Supplier<Long>> captor = ArgumentCaptor.forClass(Supplier.class);
        verify(metricsGroup).newGauge(eq("InFlightLaggingObjectBytes"), captor.capture());
        inFlight = captor.getValue();
        final ArgumentCaptor<Supplier<Long>> maxCaptor = ArgumentCaptor.forClass(Supplier.class);
        verify(metricsGroup).newGauge(eq("InFlightLaggingObjectBytesMax"), maxCaptor.capture());
        inFlightMax = maxCaptor.getValue();
    }

    @Test
    public void testTheGaugeFollowsChargesAndReleases() {
        metrics.addInFlightLaggingObjectBytes(100);
        metrics.addInFlightLaggingObjectBytes(40);
        assertThat(inFlight.get()).isEqualTo(140L);

        metrics.addInFlightLaggingObjectBytes(-140);
        assertThat(inFlight.get()).isZero();
    }

    @Test
    public void testTheMaxRatchetsOnChargesOnly() {
        metrics.addInFlightLaggingObjectBytes(100);
        assertThat(inFlightMax.get()).isEqualTo(100L);

        // A release lowers the gauge and must leave the high-water mark where it was
        metrics.addInFlightLaggingObjectBytes(-60);
        assertThat(inFlight.get()).isEqualTo(40L);
        assertThat(inFlightMax.get()).isEqualTo(100L);

        // A charge that does not exceed the mark leaves it alone too
        metrics.addInFlightLaggingObjectBytes(30);
        assertThat(inFlightMax.get()).isEqualTo(100L);

        metrics.addInFlightLaggingObjectBytes(200);
        assertThat(inFlightMax.get()).isEqualTo(270L);
    }
}
