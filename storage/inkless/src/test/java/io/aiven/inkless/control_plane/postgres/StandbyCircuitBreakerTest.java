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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;

import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.inkless.control_plane.ControlPlaneException;

import static org.assertj.core.api.Assertions.assertThat;

class StandbyCircuitBreakerTest {
    private final PostgresControlPlaneMetrics metrics = PostgresControlPlaneMetrics.instance();

    @Test
    void classifiesConnectivityAndTimeoutFailures() {
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new ControlPlaneException("wrap", new SQLException("connection failure", "08006")))).isTrue();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new SQLException("unable to connect", "08001"))).isTrue();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new SQLException("admin shutdown", "57P01"))).isTrue();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new SQLException("crash shutdown", "57P02"))).isTrue();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new SQLException("cannot connect now", "57P03"))).isTrue();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new ControlPlaneException("pool timeout", new SQLTransientConnectionException("timeout")))).isTrue();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new RuntimeException(new SocketTimeoutException("read timed out")))).isTrue();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new RuntimeException(new ConnectException("Connection refused")))).isTrue();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new RuntimeException(new UnknownHostException("standby.invalid")))).isTrue();

        final SQLException chainHead = new SQLException("outer", "99999");
        chainHead.setNextException(new SQLException("inner", "08003"));
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(chainHead)).isTrue();

        final RuntimeException withSuppressed = new RuntimeException("outer");
        withSuppressed.addSuppressed(new ConnectException("refused"));
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(withSuppressed)).isTrue();
    }

    @Test
    void excludesNonConnectivityErrorsAndHandlesCycles() {
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(null)).isFalse();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new SQLException("serialization failure", "40001"))).isFalse();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new SQLException("deadlock", "40P01"))).isFalse();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new SQLException("unique violation", "23505"))).isFalse();
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(
            new IllegalStateException("bug"))).isFalse();

        final RuntimeException a = new RuntimeException("a");
        final RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);
        assertThat(StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(a)).isFalse();
    }

    @Test
    void tripsAfterThresholdEvictsOnceAndClosesAfterProbeSuccess() throws Exception {
        final AtomicBoolean probeHealthy = new AtomicBoolean(false);
        final AtomicInteger probeAttempts = new AtomicInteger();
        final AtomicInteger evictions = new AtomicInteger();
        final CountDownLatch recovered = new CountDownLatch(1);
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try (StandbyCircuitBreaker breaker = new StandbyCircuitBreaker(
            3,
            25L,
            () -> {
                if (probeAttempts.incrementAndGet() >= 2 && probeHealthy.get()) {
                    recovered.countDown();
                    return true;
                }
                return false;
            },
            evictions::incrementAndGet,
            scheduler,
            metrics
        )) {
            assertThat(breaker.isOpen()).isFalse();
            assertThat(standbyCircuitOpenGauge()).isEqualTo(0);

            final long gen0 = breaker.generation();
            assertThat(breaker.recordFailure(gen0, new ConnectException("fail-1"))).isFalse();
            assertThat(breaker.recordFailure(gen0, new ConnectException("fail-2"))).isFalse();
            assertThat(breaker.isOpen()).isFalse();

            // Success resets consecutive failures back to zero.
            breaker.recordSuccess(gen0);
            assertThat(breaker.recordFailure(gen0, new ConnectException("fail-1b"))).isFalse();
            assertThat(breaker.recordFailure(gen0, new ConnectException("fail-2b"))).isFalse();
            assertThat(breaker.isOpen()).isFalse();
            assertThat(evictions.get()).isEqualTo(0);

            // Third consecutive failure trips OPEN and soft-evicts once.
            assertThat(breaker.recordFailure(gen0, new ConnectException("fail-3"))).isTrue();
            assertThat(breaker.isOpen()).isTrue();
            assertThat(evictions.get()).isEqualTo(1);
            assertThat(standbyCircuitOpenGauge()).isEqualTo(1);

            // Additional failures while OPEN don't re-evict.
            assertThat(breaker.recordFailure(gen0, new ConnectException("fail-4"))).isFalse();
            assertThat(evictions.get()).isEqualTo(1);

            // Allow probe to succeed and verify transition back to CLOSED.
            probeHealthy.set(true);
            assertThat(recovered.await(5, TimeUnit.SECONDS)).isTrue();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (breaker.isOpen() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(breaker.isOpen()).isFalse();
            assertThat(breaker.generation()).isEqualTo(gen0 + 1);
            assertThat(standbyCircuitOpenGauge()).isEqualTo(0);
        }
    }

    @Test
    void ignoresStaleGenerationFailuresAfterRecovery() {
        final AtomicBoolean probeHealthy = new AtomicBoolean(true);
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try (StandbyCircuitBreaker breaker = new StandbyCircuitBreaker(
            2,
            60_000L,
            probeHealthy::get,
            () -> { },
            scheduler,
            metrics
        )) {
            final long gen0 = breaker.generation();
            assertThat(breaker.recordFailure(gen0, new ConnectException("1"))).isFalse();
            assertThat(breaker.recordFailure(gen0, new ConnectException("2"))).isTrue();
            assertThat(breaker.isOpen()).isTrue();

            breaker.runProbe();
            assertThat(breaker.isOpen()).isFalse();
            assertThat(breaker.generation()).isEqualTo(gen0 + 1);

            // Late in-flight failures captured before recovery must not trip the recovered circuit.
            assertThat(breaker.recordFailure(gen0, new ConnectException("late-1"))).isFalse();
            assertThat(breaker.recordFailure(gen0, new ConnectException("late-2"))).isFalse();
            assertThat(breaker.recordFailure(gen0, new ConnectException("late-3"))).isFalse();
            assertThat(breaker.isOpen()).isFalse();
        }
    }

    @Test
    void closingOldBreakerDoesNotZeroNewBreakerGauge() {
        final ScheduledExecutorService scheduler1 = Executors.newSingleThreadScheduledExecutor();
        final ScheduledExecutorService scheduler2 = Executors.newSingleThreadScheduledExecutor();

        final StandbyCircuitBreaker oldBreaker = new StandbyCircuitBreaker(
            1, 60_000L, () -> false, () -> { }, scheduler1, metrics);
        oldBreaker.recordFailure(oldBreaker.generation(), new ConnectException("old"));
        assertThat(standbyCircuitOpenGauge()).isEqualTo(1);

        final StandbyCircuitBreaker newBreaker = new StandbyCircuitBreaker(
            1, 60_000L, () -> false, () -> { }, scheduler2, metrics);
        newBreaker.recordFailure(newBreaker.generation(), new ConnectException("new"));
        assertThat(standbyCircuitOpenGauge()).isEqualTo(1);

        // Closing the older instance must not clobber the active instance's gauge.
        oldBreaker.close();
        assertThat(standbyCircuitOpenGauge()).isEqualTo(1);

        newBreaker.close();
        assertThat(standbyCircuitOpenGauge()).isEqualTo(0);
    }

    @Test
    void closeDoesNotBlockWhenProbeIsRunning() throws Exception {
        final CountDownLatch probeStarted = new CountDownLatch(1);
        final CountDownLatch releaseProbe = new CountDownLatch(1);
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        final StandbyCircuitBreaker breaker = new StandbyCircuitBreaker(
            1,
            10L,
            () -> {
                probeStarted.countDown();
                try {
                    releaseProbe.await(30, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return false;
            },
            () -> { },
            scheduler,
            metrics
        );

        breaker.recordFailure(breaker.generation(), new ConnectException("down"));
        assertThat(probeStarted.await(5, TimeUnit.SECONDS)).isTrue();

        final long startNanos = System.nanoTime();
        breaker.close();
        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        releaseProbe.countDown();

        assertThat(elapsedMs).isLessThan(1_000L);
        assertThat(breaker.isOpen()).isFalse();
        assertThat(standbyCircuitOpenGauge()).isEqualTo(0);
    }

    @Test
    void evictionCompletesBeforeFirstProbeRuns() throws Exception {
        final AtomicBoolean evicted = new AtomicBoolean(false);
        final AtomicBoolean probeSawEvicted = new AtomicBoolean(false);
        final CountDownLatch probed = new CountDownLatch(1);
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try (StandbyCircuitBreaker breaker = new StandbyCircuitBreaker(
            1,
            1L,
            () -> {
                probeSawEvicted.set(evicted.get());
                probed.countDown();
                return true;
            },
            () -> {
                try {
                    Thread.sleep(30);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                evicted.set(true);
            },
            scheduler,
            metrics
        )) {
            assertThat(breaker.recordFailure(breaker.generation(), new ConnectException("down"))).isTrue();
            assertThat(probed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(probeSawEvicted.get()).isTrue();
        }
    }

    private static int standbyCircuitOpenGauge() {
        final KafkaMetricsGroup group = new KafkaMetricsGroup(
            PostgresControlPlane.class.getPackageName(), PostgresControlPlane.class.getSimpleName());
        return (int) ((Gauge<?>) KafkaYammerMetrics.defaultRegistry().allMetrics()
            .get(group.metricName("StandbyCircuitOpen", Map.of()))).value();
    }
}
