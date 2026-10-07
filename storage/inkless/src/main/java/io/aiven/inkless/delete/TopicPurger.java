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

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.PurgeDeletedLogsResponse;

public class TopicPurger implements Runnable, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPurger.class);

    // Shared with InklessDisklessTopicDeleteTest so the saturation assertion cannot drift.
    public static final String CAP_REACHED_LOG_FRAGMENT = "per-cycle cap reached";

    final Time time;
    final ControlPlane controlPlane;
    final int maxBatchesPerCycle;
    final TopicPurgerMetrics metrics;
    private final ExponentialBackoff errorBackoff = new ExponentialBackoff(100, 2, 60 * 1000, 0.2);
    private final AtomicInteger attempts = new AtomicInteger();
    // Do not sleep in run(): KafkaScheduler uses scheduleAtFixedRate, and a sleep queues missed
    // ticks that then fire back to back past topic.purger.max.batches.per.cycle.
    private volatile long nextEligibleMs;

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
    }

    @Override
    public void run() {
        if (time.milliseconds() < nextEligibleMs) {
            return;
        }
        try {
            metrics.recordTopicPurgerStart();
            final PurgeDeletedLogsResponse result = TimeUtils.measureDurationMs(time,
                () -> controlPlane.purgeDeletedLogs(maxBatchesPerCycle),
                metrics::recordTopicPurgerTotalTime);

            metrics.recordTopicPurgerWorkRemain(result.moreRemain());
            if (result.isEmpty()) {
                LOGGER.debug("No purge work this cycle");
            } else {
                final boolean saturated = result.capReached();
                if (saturated) {
                    metrics.recordTopicPurgerCycleSaturated();
                    LOGGER.info("Running topic purger: deleted {} batches, purged {} logs, marked {} files "
                            + "({}, more remain)",
                        result.batchesDeleted(), result.logsPurged(), result.filesMarked(),
                        CAP_REACHED_LOG_FRAGMENT);
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
            nextEligibleMs = time.milliseconds() + backoff;
            LOGGER.error("Error while purging deleted logs, skipping ticks for the next {}", Duration.ofMillis(backoff), e);
        }
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }
}
