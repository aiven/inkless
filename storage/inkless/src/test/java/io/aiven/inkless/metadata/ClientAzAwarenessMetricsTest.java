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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientAzAwarenessMetricsTest {

    @Test
    void testRecordClientAzUnaware() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        metrics.recordClientAz(null, false);
        assertEquals(0, metrics.clientAzHitRate.count());
        assertEquals(0, metrics.clientAzMissRate.count());
        assertEquals(1, metrics.clientAzUnawareRate.count());
        metrics.close();
    }

    @Test
    void testRecordClientAzMiss() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        metrics.recordClientAz("us-east-1a", false);
        assertEquals(0, metrics.clientAzHitRate.count());
        assertEquals(1, metrics.clientAzMissRate.count());
        assertEquals(0, metrics.clientAzUnawareRate.count());
        metrics.close();
    }

    @Test
    void testRecordClientAzHit() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        metrics.recordClientAz("us-east-1a", true);
        assertEquals(1, metrics.clientAzHitRate.count());
        assertEquals(0, metrics.clientAzMissRate.count());
        assertEquals(0, metrics.clientAzUnawareRate.count());
        assertTrue(metrics.clientAzHitRatesPerAz.containsKey("us-east-1a"));
        assertEquals(1, metrics.clientAzHitRatesPerAz.get("us-east-1a").count());
        metrics.close();
    }

    @Test
    void testRecordFallback() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        assertEquals(0, metrics.fallbackTotal.count());
        metrics.recordFallback();
        assertEquals(1, metrics.fallbackTotal.count());
        metrics.recordFallback();
        assertEquals(2, metrics.fallbackTotal.count());
        metrics.close();
    }

    @Test
    void testRecordOfflineReplicasRoutedAround() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        assertEquals(0, metrics.offlineReplicasRoutedAround.count());
        metrics.recordOfflineReplicasRoutedAround();
        assertEquals(1, metrics.offlineReplicasRoutedAround.count());
        metrics.recordOfflineReplicasRoutedAround();
        assertEquals(2, metrics.offlineReplicasRoutedAround.count());
        metrics.close();
    }

    @Test
    void testRecordCrossAzRouting() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        assertEquals(0, metrics.crossAzRoutingTotal.count());
        metrics.recordCrossAzRouting();
        assertEquals(1, metrics.crossAzRoutingTotal.count());
        metrics.recordCrossAzRouting();
        assertEquals(2, metrics.crossAzRoutingTotal.count());
        metrics.close();
    }

    @Test
    @Timeout(30) // Without concurrent access handling, this test may hang indefinitely
    void testConcurrentClientAzAccess() throws InterruptedException {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        final int threadCount = 21; // Use a number divisible by 3 to ensure even distribution
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(threadCount);

        // Create and start multiple threads
        for (int i = 0; i < threadCount; i++) {
            final String az = "az-" + (i % 3); // Use 3 different AZs
            new Thread(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    // Record client AZ hit multiple times
                    for (int j = 0; j < 100; j++) {
                        metrics.recordClientAz(az, true);
                    }
                    completionLatch.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }

        startLatch.countDown(); // Start all threads simultaneously
        completionLatch.await(); // Wait for all threads to complete

        // Verify results
        assertEquals(threadCount * 100, metrics.clientAzHitRate.count());
        assertEquals(3, metrics.clientAzHitRatesPerAz.size());

        // Check counts for each AZ
        for (int i = 0; i < 3; i++) {
            String az = "az-" + i;
            assertTrue(metrics.clientAzHitRatesPerAz.containsKey(az));
            assertEquals((threadCount / 3) * 100, metrics.clientAzHitRatesPerAz.get(az).count());
        }

        metrics.close();
    }
}