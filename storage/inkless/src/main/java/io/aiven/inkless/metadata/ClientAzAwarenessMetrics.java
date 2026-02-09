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
    // Client AZ matching metrics
    private static final String CLIENT_AZ_HIT_RATE = "client-az-hit-rate";
    private static final String CLIENT_AZ_MISS_RATE = "client-az-miss-rate";
    private static final String CLIENT_AZ_UNAWARE_RATE = "client-az-unaware-rate";
    // Transformer targets metrics
    private static final String FALLBACK_TOTAL = "fallback-total";
    private static final String OFFLINE_REPLICAS_ROUTED_AROUND = "offline-replicas-routed-around";
    private static final String CROSS_AZ_ROUTING_TOTAL = "cross-az-routing-total";

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(ClientAzAwarenessMetrics.class);
    final Meter clientAzHitRate;
    final Meter clientAzMissRate;
    final Map<String, Meter> clientAzHitRatesPerAz;
    final Meter clientAzUnawareRate;
    final Meter fallbackTotal;
    final Meter offlineReplicasRoutedAround;
    final Meter crossAzRoutingTotal;

    public ClientAzAwarenessMetrics() {
        clientAzUnawareRate = metricsGroup.newMeter(CLIENT_AZ_UNAWARE_RATE, "requests", TimeUnit.SECONDS, Map.of());
        clientAzMissRate = metricsGroup.newMeter(CLIENT_AZ_MISS_RATE, "requests", TimeUnit.SECONDS, Map.of());
        clientAzHitRate = metricsGroup.newMeter(CLIENT_AZ_HIT_RATE, "requests", TimeUnit.SECONDS, Map.of());
        clientAzHitRatesPerAz = new ConcurrentHashMap<>(3);
        fallbackTotal = metricsGroup.newMeter(FALLBACK_TOTAL, "requests", TimeUnit.SECONDS, Map.of());
        offlineReplicasRoutedAround = metricsGroup.newMeter(OFFLINE_REPLICAS_ROUTED_AROUND, "requests", TimeUnit.SECONDS, Map.of());
        crossAzRoutingTotal = metricsGroup.newMeter(CROSS_AZ_ROUTING_TOTAL, "requests", TimeUnit.SECONDS, Map.of());
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

    /**
     * Record when routing falls back to a non-replica broker.
     * This only happens for diskless-only topics (remote storage disabled).
     */
    public void recordFallback() {
        fallbackTotal.mark();
    }

    /**
     * Record when routing around offline replicas to find an available leader.
     * Indicates some replicas were offline but an alternative was found.
     */
    public void recordOfflineReplicasRoutedAround() {
        offlineReplicasRoutedAround.mark();
    }

    /**
     * Record when routing to a broker in a different AZ than the client.
     */
    public void recordCrossAzRouting() {
        crossAzRoutingTotal.mark();
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(CLIENT_AZ_HIT_RATE);
        metricsGroup.removeMetric(CLIENT_AZ_MISS_RATE);
        metricsGroup.removeMetric(CLIENT_AZ_UNAWARE_RATE);
        metricsGroup.removeMetric(FALLBACK_TOTAL);
        metricsGroup.removeMetric(OFFLINE_REPLICAS_ROUTED_AROUND);
        metricsGroup.removeMetric(CROSS_AZ_ROUTING_TOTAL);
        clientAzHitRatesPerAz.keySet().forEach(az -> metricsGroup.removeMetric(CLIENT_AZ_HIT_RATE, Map.of("az", az)));
    }
}
