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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ClientAzAwarenessMetrics implements Closeable {
    private static final String GROUP = ClientAzAwarenessMetrics.class.getSimpleName();

    // Client AZ matching metrics
    private static final String CLIENT_AZ_HIT_RATE = "client-az-hit-rate";
    private static final String CLIENT_AZ_HIT_RATE_DOC = "Rate of requests routed to a broker in the same availability zone as the client";
    private static final String CLIENT_AZ_MISS_RATE = "client-az-miss-rate";
    private static final String CLIENT_AZ_MISS_RATE_DOC = "Rate of requests where no broker was available in the client availability zone";
    private static final String CLIENT_AZ_UNAWARE_RATE = "client-az-unaware-rate";
    private static final String CLIENT_AZ_UNAWARE_RATE_DOC = "Rate of requests from clients without availability zone information";
    // Transformer targets metrics
    private static final String FALLBACK_TOTAL = "fallback-total";
    private static final String FALLBACK_TOTAL_DOC = "Rate of requests routed to non-replica brokers (diskless-only topics)";
    private static final String OFFLINE_REPLICAS_ROUTED_AROUND = "offline-replicas-routed-around";
    private static final String OFFLINE_REPLICAS_ROUTED_AROUND_DOC = "Rate of requests rerouted around offline replicas";
    private static final String CROSS_AZ_ROUTING_TOTAL = "cross-az-routing-total";
    private static final String CROSS_AZ_ROUTING_TOTAL_DOC = "Rate of requests routed to a broker in a different availability zone";
    // Tags
    public static final String CLIENT_AZ_TAG = "client-az";

    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(CLIENT_AZ_HIT_RATE, GROUP, CLIENT_AZ_HIT_RATE_DOC),
            new MetricNameTemplate(CLIENT_AZ_MISS_RATE, GROUP, CLIENT_AZ_MISS_RATE_DOC),
            new MetricNameTemplate(CLIENT_AZ_UNAWARE_RATE, GROUP, CLIENT_AZ_UNAWARE_RATE_DOC),
            new MetricNameTemplate(FALLBACK_TOTAL, GROUP, FALLBACK_TOTAL_DOC),
            new MetricNameTemplate(OFFLINE_REPLICAS_ROUTED_AROUND, GROUP, OFFLINE_REPLICAS_ROUTED_AROUND_DOC),
            new MetricNameTemplate(CROSS_AZ_ROUTING_TOTAL, GROUP, CROSS_AZ_ROUTING_TOTAL_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        ClientAzAwarenessMetrics.class.getPackageName(), ClientAzAwarenessMetrics.class.getSimpleName());
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
            Map.of(CLIENT_AZ_TAG, clientAZ)
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
        clientAzHitRatesPerAz.keySet().forEach(az -> metricsGroup.removeMetric(CLIENT_AZ_HIT_RATE, Map.of(CLIENT_AZ_TAG, az)));
    }
}
