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

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.inkless.control_plane.ControlPlaneException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link JobUtils} retry behavior. These use {@link MockTime}, so the backoff
 * {@code sleep} advances virtual time only and the tests never block on wall-clock.
 */
@Timeout(10)
class JobUtilsTest {

    /** Serialization failure / recovery conflict. PostgreSQL uses 40001 for both. */
    private static SQLException recoveryConflict() {
        return new SQLException("canceling statement due to conflict with recovery", "40001");
    }

    private static SQLException deadlock() {
        return new SQLException("deadlock detected", "40P01");
    }

    @Test
    void succeedsWithoutRetryWhenNoError() {
        final MockTime time = new MockTime();
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger durationCallbacks = new AtomicInteger();
        final AtomicInteger reportedDurationMs = new AtomicInteger(-1);
        final long startMs = time.milliseconds();

        final String result = JobUtils.run(() -> {
            attempts.incrementAndGet();
            return "ok";
        }, time, d -> {
            durationCallbacks.incrementAndGet();
            reportedDurationMs.set(d.intValue());
        });

        assertThat(result).isEqualTo("ok");
        assertThat(attempts).hasValue(1);
        assertThat(durationCallbacks).hasValue(1);
        assertThat(reportedDurationMs).hasValue(0);
        assertThat(time.milliseconds()).isEqualTo(startMs);
    }

    @Test
    void retriesRecoveryConflictThenSucceeds() {
        final MockTime time = new MockTime();
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger durationCallbacks = new AtomicInteger();
        final AtomicInteger reportedDurationMs = new AtomicInteger(-1);
        final long startMs = time.milliseconds();

        // Reproduces the production incident: the read replica cancels the first attempts with a
        // recovery conflict (SQLState 40001), wrapped exactly as the control plane wraps it, then a
        // later attempt on a fresh pooled connection succeeds.
        final Callable<String> flaky = () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new ControlPlaneException("Error finding batches",
                    new RuntimeException("Cannot commit transaction", recoveryConflict()));
            }
            return "recovered";
        };

        final String result = JobUtils.run(flaky, time, d -> {
            durationCallbacks.incrementAndGet();
            reportedDurationMs.set(d.intValue());
        });

        assertThat(result).isEqualTo("recovered");
        assertThat(attempts).hasValue(3);
        assertThat(durationCallbacks).hasValue(1);
        // QueryTime is the whole run, including backoff, so a retry storm shows up as latency.
        assertThat(reportedDurationMs).hasValue((int) (time.milliseconds() - startMs));
        assertThat(reportedDurationMs.get()).isGreaterThan(0);
    }

    @Test
    void backsOffWithExponentialJitterBetweenRetries() {
        final MockTime time = new MockTime();
        final AtomicInteger attempts = new AtomicInteger();
        final long startMs = time.milliseconds();

        // Fails on the first two attempts, succeeds on the third, so two backoff sleeps happen.
        JobUtils.run(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException(recoveryConflict());
            }
            return "ok";
        }, time, ignored -> { });

        // Jittered exponential backoff: attempt 1 sleeps ~50ms, attempt 2 sleeps ~100ms, each
        // scaled by a factor in [1 - JITTER, 1 + JITTER]. Assert the total elapsed virtual time
        // falls within those combined bounds (this also proves a sleep actually happened).
        final long elapsed = time.milliseconds() - startMs;
        final long nominal = JobUtils.INITIAL_BACKOFF_MS
            + JobUtils.INITIAL_BACKOFF_MS * JobUtils.BACKOFF_MULTIPLIER;
        final long minExpected = (long) (nominal * (1 - JobUtils.BACKOFF_JITTER));
        final long maxExpected = (long) (nominal * (1 + JobUtils.BACKOFF_JITTER));
        assertThat(elapsed).isBetween(minExpected, maxExpected);
    }

    @Test
    void retriesDeadlock() {
        final MockTime time = new MockTime();
        final AtomicInteger attempts = new AtomicInteger();

        final String result = JobUtils.run(() -> {
            if (attempts.incrementAndGet() < 2) {
                throw new RuntimeException(deadlock());
            }
            return "ok";
        }, time, ignored -> { });

        assertThat(result).isEqualTo("ok");
        assertThat(attempts).hasValue(2);
    }

    @Test
    void stopsAfterMaxAttemptsAndRethrowsOriginal() {
        final MockTime time = new MockTime();
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger durationCallbacks = new AtomicInteger();
        final AtomicInteger reportedDurationMs = new AtomicInteger(-1);
        final long startMs = time.milliseconds();

        // Always fails with a retriable error: the caller must observe the unchanged
        // ControlPlaneException contract after retries are exhausted.
        assertThatThrownBy(() -> JobUtils.run((Callable<String>) () -> {
            attempts.incrementAndGet();
            throw new ControlPlaneException("Error finding batches",
                new RuntimeException(recoveryConflict()));
        }, time, d -> {
            durationCallbacks.incrementAndGet();
            reportedDurationMs.set(d.intValue());
        }))
            .isInstanceOf(ControlPlaneException.class)
            .hasMessage("Error finding batches");

        assertThat(attempts).hasValue(JobUtils.MAX_ATTEMPTS);
        assertThat(durationCallbacks).hasValue(1);
        assertThat(reportedDurationMs).hasValue((int) (time.milliseconds() - startMs));
        assertThat(reportedDurationMs.get()).isGreaterThan(0);
    }

    @Test
    void doesNotRetryNonRetriableSqlState() {
        final MockTime time = new MockTime();
        final AtomicInteger attempts = new AtomicInteger();
        final long startMs = time.milliseconds();

        // 23505 (unique_violation) is a deterministic error; retrying would just fail again.
        assertThatThrownBy(() -> JobUtils.run((Callable<String>) () -> {
            attempts.incrementAndGet();
            throw new RuntimeException(new SQLException("duplicate key", "23505"));
        }, time, ignored -> { }))
            .isInstanceOf(RuntimeException.class);

        assertThat(attempts).hasValue(1);
        // A deterministic failure must fail fast: no backoff sleep before giving up.
        assertThat(time.milliseconds()).isEqualTo(startMs);
    }

    @Test
    void doesNotRetryPlainRuntimeException() {
        final MockTime time = new MockTime();
        final AtomicInteger attempts = new AtomicInteger();
        final long startMs = time.milliseconds();

        assertThatThrownBy(() -> JobUtils.run((Callable<String>) () -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("bug, not transient");
        }, time, ignored -> { }))
            .isInstanceOf(RuntimeException.class);

        assertThat(attempts).hasValue(1);
        assertThat(time.milliseconds()).isEqualTo(startMs);
    }

    @Test
    void abandonsRetriesWhenInterruptedDuringBackoff() {
        final MockTime time = new MockTime();
        final AtomicInteger attempts = new AtomicInteger();

        // Set the interrupt flag so that after the first backoff the loop observes an interrupted
        // thread (mirrors a broker shutdown interrupting the retry) and stops immediately instead
        // of exhausting all attempts.
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> JobUtils.run((Callable<String>) () -> {
                attempts.incrementAndGet();
                throw new RuntimeException(recoveryConflict());
            }, time, ignored -> { }))
                .isInstanceOf(RuntimeException.class);

            // Only the first attempt ran; the interrupt aborted the retry loop before a second try.
            assertThat(attempts).hasValue(1);
            // The interrupt status is preserved for cooperative shutdown downstream.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clear the flag so it does not leak into other tests.
            Thread.interrupted();
        }
    }

    @Test
    void wrapsNonControlPlaneExceptionInRuntimeException() {
        final MockTime time = new MockTime();

        assertThatThrownBy(() -> JobUtils.run((Callable<String>) () -> {
            throw new SQLException("non-retriable", "08006");
        }, time, ignored -> { }))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(SQLException.class);
    }

    @Test
    void isRetriableScansSuppressedThrowables() {
        // The connection-terminated form surfaces the retriable cause as a suppressed throwable
        // (the failed rollback), not on the main cause chain.
        final RuntimeException top = new RuntimeException("Cannot commit transaction");
        top.addSuppressed(new SQLException("terminating connection due to conflict with recovery", "40001"));

        assertThat(JobUtils.isRetriable(top)).isTrue();
    }

    @Test
    void isRetriableScansSqlExceptionNextException() {
        // JDBC chains related errors via getNextException(), separate from getCause().
        final SQLException head = new SQLException("connection closed", "08006");
        head.setNextException(recoveryConflict());

        assertThat(JobUtils.isRetriable(head)).isTrue();
        assertThat(JobUtils.isRetriable(new RuntimeException(head))).isTrue();
    }

    @Test
    void isRetriableHandlesSelfReferentialCauseChain() {
        final SQLException a = new SQLException("a", "23505");
        final SQLException b = new SQLException("b", "23505");
        a.initCause(b);
        b.initCause(a);

        // Must terminate, not StackOverflow, and report no retriable state.
        assertThat(JobUtils.isRetriable(a)).isFalse();
    }

    @Test
    void isRetriableHandlesSelfReferentialNextExceptionChain() {
        final SQLException a = new SQLException("a", "23505");
        final SQLException b = new SQLException("b", "23505");
        a.setNextException(b);
        b.setNextException(a);

        assertThat(JobUtils.isRetriable(a)).isFalse();
    }
}
