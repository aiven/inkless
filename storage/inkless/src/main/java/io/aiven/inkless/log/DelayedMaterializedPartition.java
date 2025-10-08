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
package io.aiven.inkless.log;

import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Time;

/**
 * {@link Delayed} wrapper over {@link MaterializedPartition}.
 */
class DelayedMaterializedPartition implements Delayed {
    private final Time time;
    private final long endTimeNanos;
    final MaterializedPartition partition;

    DelayedMaterializedPartition(final Time time, final long delayMs, final MaterializedPartition partition) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.partition = Objects.requireNonNull(partition, "partition cannot be null");
        this.endTimeNanos = time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(delayMs);
    }

    @Override
    public int compareTo(final Delayed other) {
        Objects.requireNonNull(other, "other cannot be null");
        final DelayedMaterializedPartition otherChannel = (DelayedMaterializedPartition) other;
        return Long.compare(this.endTimeNanos, otherChannel.endTimeNanos);
    }

    @Override
    public long getDelay(final TimeUnit unit) {
        return unit.convert(endTimeNanos - time.nanoseconds(), TimeUnit.NANOSECONDS);
    }
}
