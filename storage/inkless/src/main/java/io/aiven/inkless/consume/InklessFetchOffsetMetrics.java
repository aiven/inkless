/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.util.Map;
import java.util.Objects;

public class InklessFetchOffsetMetrics {
    private static final String FETCH_OFFSET_TOTAL_TIME = "FetchOffsetTotalTime";
    private static final String FETCH_OFFSET_RATE = "FetchOffsetRate";
    private static final String FETCH_OFFSET_ERROR_RATE = "FetchOffsetErrorRate";

    private final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(InklessFetchOffsetMetrics.class);

    private final Histogram fetchOffsetTimeHistogram;
    private final Meter fetchOffsetRate;
    private final Meter fetchOffsetErrorRate;

    public InklessFetchOffsetMetrics(Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        fetchOffsetTimeHistogram = metricsGroup.newHistogram(FETCH_OFFSET_TOTAL_TIME, true, Map.of());
        fetchOffsetRate = metricsGroup.newMeter(FETCH_OFFSET_RATE, "fetches", java.util.concurrent.TimeUnit.SECONDS, Map.of());
        fetchOffsetErrorRate = metricsGroup.newMeter(FETCH_OFFSET_ERROR_RATE, "errors", java.util.concurrent.TimeUnit.SECONDS, Map.of());
    }

    public void fetchOffsetCompleted(final long durationMs) {
        fetchOffsetTimeHistogram.update(durationMs);
        fetchOffsetRate.mark();
    }

    public void fetchOffsetFailed() {
        fetchOffsetErrorRate.mark();
    }
}
