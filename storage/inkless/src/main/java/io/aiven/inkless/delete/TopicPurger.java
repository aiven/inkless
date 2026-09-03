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

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.PurgeDeletedLogsResponse;

public class TopicPurger implements Runnable, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPurger.class);

    final Time time;
    final ControlPlane controlPlane;
    final int maxBatchesPerCycle;
    final TopicPurgerMetrics metrics;
    private final ExponentialBackoff errorBackoff = new ExponentialBackoff(100, 2, 60 * 1000, 0.2);
    private final Supplier<Long> noWorkBackoffSupplier;
    private final AtomicInteger attempts = new AtomicInteger();

    public TopicPurger(SharedState sharedState) {
        this(
            sharedState.time(),
            sharedState.controlPlane(),
            sharedState.config().topicPurgerMaxBatchesPerCycle()
        );
    }

    // package-private constructor for testing
    TopicPurger(Time time,
                ControlPlane controlPlane,
                int maxBatchesPerCycle) {
        this.time = time;
        this.controlPlane = controlPlane;
        this.maxBatchesPerCycle = maxBatchesPerCycle;
        this.metrics = new TopicPurgerMetrics(time);

        final int noWorkBackoffDuration = 10 * 1000;
        final var noWorkBackoff = new ExponentialBackoff(noWorkBackoffDuration, 1, noWorkBackoffDuration * 2, 0.2);
        noWorkBackoffSupplier = () -> noWorkBackoff.backoff(1);
    }

    @Override
    public void run() {
        try {
            metrics.recordTopicPurgerStart();
            final PurgeDeletedLogsResponse result = TimeUtils.measureDurationMs(time,
                () -> controlPlane.purgeDeletedLogs(maxBatchesPerCycle),
                metrics::recordTopicPurgerTotalTime);

            metrics.recordTopicPurgerWorkRemain(result.moreRemain());
            if (result.isEmpty()) {
                final long sleepMillis = noWorkBackoffSupplier.get();
                LOGGER.info("No purge work this cycle, sleeping for {}", Duration.ofMillis(sleepMillis));
                time.sleep(sleepMillis);
            } else {
                final boolean saturated = maxBatchesPerCycle > 0
                    && result.moreRemain()
                    && result.batchesDeleted() >= maxBatchesPerCycle;
                if (saturated) {
                    metrics.recordTopicPurgerCycleSaturated();
                    LOGGER.info("Running topic purger: deleted {} batches, purged {} logs, marked {} files "
                            + "(per-cycle cap reached, more remain)",
                        result.batchesDeleted(), result.logsPurged(), result.filesMarked());
                } else {
                    LOGGER.info("Running topic purger: deleted {} batches, purged {} logs, marked {} files",
                        result.batchesDeleted(), result.logsPurged(), result.filesMarked());
                }
                metrics.recordTopicPurgerCompleted(result.batchesDeleted(), result.logsPurged(), result.filesMarked());
            }

            attempts.set(0);
            metrics.recordTopicPurgerCycleSucceeded();
        } catch (final Exception e) {
            metrics.recordTopicPurgerError();
            final long backoff = errorBackoff.backoff(attempts.incrementAndGet());
            LOGGER.error("Error while purging deleted logs, waiting for {}", Duration.ofMillis(backoff), e);
            time.sleep(backoff);
        }
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }
}
