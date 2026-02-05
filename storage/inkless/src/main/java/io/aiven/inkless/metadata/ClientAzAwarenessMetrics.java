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
package io.aiven.inkless.metadata;

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ClientAzAwarenessMetrics implements Closeable {
    private static final String CLIENT_AZ_HIT_RATE = "client-az-hit-rate";
    private static final String CLIENT_AZ_MISS_RATE = "client-az-miss-rate";
    private static final String CLIENT_AZ_UNAWARE_RATE = "client-az-unaware-rate";

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        ClientAzAwarenessMetrics.class.getPackageName(), ClientAzAwarenessMetrics.class.getSimpleName());
    final Meter clientAzHitRate;
    final Meter clientAzMissRate;
    final Map<String, Meter> clientAzHitRatesPerAz;
    final Meter clientAzUnawareRate;

    public ClientAzAwarenessMetrics() {
        clientAzUnawareRate = metricsGroup.newMeter(CLIENT_AZ_UNAWARE_RATE, "requests", TimeUnit.SECONDS, Map.of());
        clientAzMissRate = metricsGroup.newMeter(CLIENT_AZ_MISS_RATE, "requests", TimeUnit.SECONDS, Map.of());
        clientAzHitRate = metricsGroup.newMeter(CLIENT_AZ_HIT_RATE, "requests", TimeUnit.SECONDS, Map.of());
        clientAzHitRatesPerAz = new ConcurrentHashMap<>(3);
    }

    public void recordClientAz(String clientAZ, boolean foundBrokersInClientAZ) {
        if (clientAZ == null) {
            clientAzUnawareRate.mark();
            return;
        }
        if (!foundBrokersInClientAZ) {
            clientAzMissRate.mark();
            return;
        }
        clientAzHitRate.mark();
        clientAzHitRatesPerAz.computeIfAbsent(clientAZ, az -> metricsGroup.newMeter(
            CLIENT_AZ_HIT_RATE,
            "requests",
            TimeUnit.SECONDS,
            Map.of("client-az", clientAZ)
        )).mark();
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(CLIENT_AZ_HIT_RATE);
        metricsGroup.removeMetric(CLIENT_AZ_MISS_RATE);
        metricsGroup.removeMetric(CLIENT_AZ_UNAWARE_RATE);
        clientAzHitRatesPerAz.keySet().forEach(az -> metricsGroup.removeMetric(CLIENT_AZ_HIT_RATE, Map.of("az", az)));
    }
}
