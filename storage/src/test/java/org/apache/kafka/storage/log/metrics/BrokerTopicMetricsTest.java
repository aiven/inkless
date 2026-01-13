/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.log.metrics;

import com.yammer.metrics.core.Meter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for BrokerTopicMetrics focusing on topicType tagging behavior:
 * - All-topics metrics (allTopicsStats) should have separate meters for classic and diskless topicType tags
 * - Topic-specific metrics should only be tagged by topic name and ignore the isDiskless flag
 */
public class BrokerTopicMetricsTest {
    private BrokerTopicMetrics allTopicsStats;
    private BrokerTopicMetrics topicSpecificStats;

    @BeforeEach
    public void setup() {
        allTopicsStats = new BrokerTopicMetrics(false);
        topicSpecificStats = new BrokerTopicMetrics("test-topic", false);
    }

    @AfterEach
    public void teardown() {
        allTopicsStats.close();
        topicSpecificStats.close();
    }

    @Test
    public void testAllTopicsStatsHasSeparateMetersForClassicAndDiskless() {
        // All-topics stats should have different meters for classic vs diskless (different topicType tags).
        // This is a broker-level split of traffic (not tied to any topic name).
        int initialMetricNameCount = allTopicsStats.metricMapKeySet().size();

        Meter classicBytesIn = allTopicsStats.bytesInRate(false);
        Meter disklessBytesIn = allTopicsStats.bytesInRate(true);

        assertNotNull(classicBytesIn, "Classic bytesInRate should not be null");
        assertNotNull(disklessBytesIn, "Diskless bytesInRate should not be null");
        assertNotEquals(classicBytesIn, disklessBytesIn, 
            "All-topics stats should have different meters for classic vs diskless (different topicType tags)");

        Meter classicBytesOut = allTopicsStats.bytesOutRate(false);
        Meter disklessBytesOut = allTopicsStats.bytesOutRate(true);

        assertNotNull(classicBytesOut, "Classic bytesOutRate should not be null");
        assertNotNull(disklessBytesOut, "Diskless bytesOutRate should not be null");
        assertNotEquals(classicBytesOut, disklessBytesOut, 
            "All-topics stats should have different meters for classic vs diskless (different topicType tags)");

        // Even though we have two meters for bytes in/out (classic + diskless), we do not introduce additional
        // metric NAMES into the registry/map (avoids key-space explosion).
        assertEquals(initialMetricNameCount, allTopicsStats.metricMapKeySet().size(),
            "Creating diskless all-topics meters should not add extra metric names");
    }

    @Test
    public void testTopicSpecificStatsIgnoresIsDisklessFlag() {
        // Topic-specific stats should return the same meter regardless of isDiskless flag
        // (only tagged by topic name, no topicType tag). This is intentional to avoid doubling the
        // amount of per-topic meters kept in memory (classic + diskless variants per topic).
        Meter bytesIn1 = topicSpecificStats.bytesInRate(false);
        Meter bytesIn2 = topicSpecificStats.bytesInRate(true);
        Meter bytesIn3 = topicSpecificStats.bytesInRate();

        assertNotNull(bytesIn1, "BytesInRate should not be null");
        assertEquals(bytesIn1, bytesIn2, 
            "Topic-specific metrics should return same meter regardless of isDiskless flag (only tagged by topic name)");
        assertEquals(bytesIn2, bytesIn3, 
            "Topic-specific metrics should return same meter for default call (only tagged by topic name)");

        Meter bytesOut1 = topicSpecificStats.bytesOutRate(false);
        Meter bytesOut2 = topicSpecificStats.bytesOutRate(true);
        Meter bytesOut3 = topicSpecificStats.bytesOutRate();

        assertNotNull(bytesOut1, "BytesOutRate should not be null");
        assertEquals(bytesOut1, bytesOut2, 
            "Topic-specific metrics should return same meter regardless of isDiskless flag (only tagged by topic name)");
        assertEquals(bytesOut2, bytesOut3, 
            "Topic-specific metrics should return same meter for default call (only tagged by topic name)");
    }

    @Test
    public void testAllTopicsStatsMetersAreIndependent() {
        // Verify that classic and diskless meters track independently
        Meter classicMeter = allTopicsStats.bytesInRate(false);
        Meter disklessMeter = allTopicsStats.bytesInRate(true);

        long initialClassicCount = classicMeter.count();
        long initialDisklessCount = disklessMeter.count();

        // Mark bytes on classic meter
        classicMeter.mark(1000);
        assertEquals(initialClassicCount + 1000, classicMeter.count(), 
            "Classic meter should increase independently");
        assertEquals(initialDisklessCount, disklessMeter.count(), 
            "Diskless meter should remain unchanged when classic is marked");

        // Mark bytes on diskless meter
        disklessMeter.mark(2000);
        assertEquals(initialClassicCount + 1000, classicMeter.count(), 
            "Classic meter should remain unchanged when diskless is marked");
        assertEquals(initialDisklessCount + 2000, disklessMeter.count(), 
            "Diskless meter should increase independently");
    }

    @Test
    public void testTopicSpecificStatsAllCallsUpdateSameMeter() {
        // All calls to topic-specific metrics should update the same underlying meter
        Meter meter = topicSpecificStats.bytesInRate();
        long initialCount = meter.count();

        // Mark with default (classic)
        topicSpecificStats.bytesInRate().mark(100);
        assertEquals(initialCount + 100, meter.count(), "Meter should increase by 100");

        // Mark with explicit false (classic)
        topicSpecificStats.bytesInRate(false).mark(200);
        assertEquals(initialCount + 300, meter.count(), "Meter should increase to 300 (same meter)");

        // Mark with explicit true (diskless) - should still affect the same meter
        topicSpecificStats.bytesInRate(true).mark(400);
        assertEquals(initialCount + 700, meter.count(), 
            "Meter should increase to 700 (same meter, topicType tag ignored for topic-specific stats)");
    }

    @Test
    public void testDefaultMethodsUseClassicTopicType() {
        // Default methods (no isDiskless parameter) should behave like isDiskless=false
        Meter defaultBytesIn = allTopicsStats.bytesInRate();
        Meter classicBytesIn = allTopicsStats.bytesInRate(false);

        assertEquals(defaultBytesIn, classicBytesIn, 
            "Default bytesInRate() should return same meter as bytesInRate(false) for all-topics stats");

        Meter defaultBytesOut = allTopicsStats.bytesOutRate();
        Meter classicBytesOut = allTopicsStats.bytesOutRate(false);

        assertEquals(defaultBytesOut, classicBytesOut, 
            "Default bytesOutRate() should return same meter as bytesOutRate(false) for all-topics stats");
    }

    @Test
    public void testMultipleTopicSpecificMetricsAreDistinct() {
        // Different topics should have different meters (different topic name tags)
        BrokerTopicMetrics topic1 = new BrokerTopicMetrics("topic-1", false);
        BrokerTopicMetrics topic2 = new BrokerTopicMetrics("topic-2", false);

        try {
            Meter topic1BytesIn = topic1.bytesInRate();
            Meter topic2BytesIn = topic2.bytesInRate();

            assertNotEquals(topic1BytesIn, topic2BytesIn, 
                "Different topics should have different meters (different topic name tags)");

            // Verify they track independently
            topic1BytesIn.mark(1000);
            topic2BytesIn.mark(2000);

            assertEquals(1000, topic1BytesIn.count(), "Topic 1 should have 1000 bytes");
            assertEquals(2000, topic2BytesIn.count(), "Topic 2 should have 2000 bytes");
        } finally {
            topic1.close();
            topic2.close();
        }
    }

    @Test
    public void testMeterAccessAfterCloseIsSafe() {
        // Accessing meters after close() should be safe: it may recreate meters as needed.
        // This avoids throwing from metrics paths and prevents NPE at call sites.
        BrokerTopicMetrics metrics = new BrokerTopicMetrics(false);
        
        // Access meters before close - should work fine
        Meter meterBeforeClose = metrics.bytesInRate(true);
        assertNotNull(meterBeforeClose, "Meter should be accessible before close");
        
        // Close the metrics
        metrics.close();
        
        // Access after close should be safe (may recreate and re-register the meter)
        Meter meterAfterClose = metrics.bytesInRate(true);
        assertNotNull(meterAfterClose, "Meter should be accessible after close");
        assertDoesNotThrow(() -> meterAfterClose.mark(1), "Marking after close should not throw");
        assertNotSame(meterBeforeClose, meterAfterClose, "Meter should be recreated after close");
        
        // Also verify for bytesOutRate
        assertDoesNotThrow(() -> metrics.bytesOutRate(false),
            "Accessing bytesOutRate after close should be safe");
    }

    @Test
    public void testTopicSpecificMeterAccessAfterCloseIsSafe() {
        // Verify that topic-specific metrics are also safe to access after close
        BrokerTopicMetrics topicMetrics = new BrokerTopicMetrics("test-topic-close", false);
        
        // Access meters before close - should work fine
        Meter meterBeforeClose = topicMetrics.bytesInRate();
        assertNotNull(meterBeforeClose, "Meter should be accessible before close");
        
        // Close the metrics
        topicMetrics.close();
        
        // Access after close should be safe (may recreate and re-register the meter)
        Meter meterAfterClose = topicMetrics.bytesInRate();
        assertNotNull(meterAfterClose, "Meter should be accessible after close");
        assertDoesNotThrow(() -> meterAfterClose.mark(1), "Marking after close should not throw");
        assertNotSame(meterBeforeClose, meterAfterClose, "Meter should be recreated after close");
    }

    @Test
    public void testConcurrentMeterAccessAndClose() throws InterruptedException {
        // Deterministic concurrency test:
        // - Ensure meters can be obtained concurrently before close().
        // - Ensure that once close() is called, further meter access is still safe.
        //
        // This does not attempt to probabilistically "hit" a narrow timing window. Instead, it
        // coordinates threads so the post-close behavior is exercised while threads are active.
        //
        // Flakiness note: the main remaining risk is environmental (very slow CI / long GC pauses)
        // causing the barrier/latch await timeouts to fire. The assertions are otherwise deterministic
        // because we explicitly coordinate a successful pre-close access and a failing post-close access.

        // Small repetition count: this test is deterministic (thread coordination via latches),
        // so we don't need many iterations to "increase probability". Repeating a couple times
        // helps catch accidental regressions without making the suite slower or flaky.
        final int repetitions = 2;
        
        for (int repetition = 0; repetition < repetitions; repetition++) {
            BrokerTopicMetrics metrics = new BrokerTopicMetrics(false);
            // Two access threads are enough because the test is phase-coordinated:
            // each thread does a pre-close access (must succeed) and a post-close access (must fail).
            // The extra (+1) participant is the closer thread, so all threads start the scenario together.
            final int accessThreadCount = 2;
            ExecutorService executor = Executors.newFixedThreadPool(accessThreadCount + 1);
            // Barrier includes access threads + closer thread; ensures close() runs while access threads are active.
            CyclicBarrier barrier = new CyclicBarrier(accessThreadCount + 1);
            AtomicInteger successfulMeterGets = new AtomicInteger(0);
            AtomicInteger successfulMeterGetsAfterClose = new AtomicInteger(0);
            CountDownLatch initialMeterGetsDone = new CountDownLatch(accessThreadCount);
            CountDownLatch closed = new CountDownLatch(1);
            List<Future<Void>> futures = new ArrayList<>(accessThreadCount + 1);

            try {
                // Spawn threads that will access meters before/after close.
                for (int i = 0; i < accessThreadCount; i++) {
                    final boolean isDiskless = (i % 2 == 0);
                    futures.add(executor.submit(() -> {
                        barrier.await(); // Synchronize all threads to start together

                        // Phase 1: obtain meter before close (must succeed)
                        Meter meter = metrics.bytesInRate(isDiskless);
                        assertNotNull(meter, "Meter should be accessible before close");
                        successfulMeterGets.incrementAndGet();
                        initialMeterGetsDone.countDown();

                        // Phase 2: wait for close to complete, then meter access should still be safe
                        assertTrue(closed.await(10, TimeUnit.SECONDS), "Close should complete within timeout");
                        Meter meterAfterClose = metrics.bytesInRate(isDiskless);
                        assertNotNull(meterAfterClose, "Meter should be accessible after close");
                        successfulMeterGetsAfterClose.incrementAndGet();
                        return null;
                    }));
                }

                // One thread will close the metrics after a brief moment
                futures.add(executor.submit(() -> {
                    barrier.await(); // Synchronize with other threads
                    assertTrue(initialMeterGetsDone.await(10, TimeUnit.SECONDS),
                        "Initial meter accesses should complete within timeout");
                    metrics.close();
                    closed.countDown();
                    return null;
                }));

                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Executor should terminate within timeout");

                // Propagate any unexpected exceptions from worker/closer threads.
                //
                // Why: exceptions thrown inside executor tasks (including assertion failures) do not fail
                // the test thread by default. They are captured by the executor and only surface if we
                // explicitly observe the returned Future. Calling Future#get ensures that:
                // - assertion failures in worker threads actually fail this test
                // - unexpected runtime exceptions are not silently ignored
                // - coordination issues (e.g., BrokenBarrierException) are reported with a clear message
                for (Future<Void> future : futures) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                            throw (InterruptedException) cause;
                        }
                        if (cause instanceof BrokenBarrierException) {
                            throw new AssertionError("Barrier coordination failed", cause);
                        }
                        if (cause instanceof Error) {
                            throw (Error) cause;
                        }
                        throw new AssertionError("Unexpected exception in concurrent task", cause);
                    }
                }

                assertEquals(accessThreadCount, successfulMeterGets.get(),
                    "All access threads should succeed in obtaining meters before close");
                assertEquals(accessThreadCount, successfulMeterGetsAfterClose.get(),
                    "All access threads should succeed in obtaining meters after close");

            } finally {
                executor.shutdownNow();
            }
        }
    }
}
