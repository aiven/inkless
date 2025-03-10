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
package io.aiven.inkless;

import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class TimeUtils {
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    public static Instant now(final Time time) {
        return Instant.ofEpochMilli(time.milliseconds());
    }

    /**
     * Return the {@link Instant} that represents the current moment according to {@link Time#nanoseconds()}.
     *
     * <p>This is supposed to be used for duration measurements only as it isn't linked to wall clock.
     */
    public static Instant durationMeasurementNow(final Time time) {
        final long nowNano = time.nanoseconds();
        return Instant.ofEpochSecond(nowNano / NANOS_PER_SECOND, nowNano % NANOS_PER_SECOND);
    }

    /**
     * Measure the duration of a {@link Callable}.
     */
    public static <V> V measureDurationMs(final Time time, final Callable<V> f, final Consumer<Long> callback) throws Exception {
        final Instant start = TimeUtils.durationMeasurementNow(time);
        try {
            return f.call();
        } finally {
            final Instant now = TimeUtils.durationMeasurementNow(time);
            callback.accept(Duration.between(start, now).toMillis());
        }
    }

    /**
     * Measure the duration of a {@link Runnable}.
     */
    public static void measureDurationMs(final Time time, final Runnable f, final Consumer<Long> callback) {
        try {
            measureDurationMs(time,
                () -> {
                    f.run();
                    return null;
                },
                callback);
        } catch (final Exception e) {
            // The passed Runnable is not supposed to throw any checked exception,
            // so this is just to make the exception checker happy.
            throw new RuntimeException(e);
        }
    }
}
