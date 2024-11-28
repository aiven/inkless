// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless;

import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class TimeUtils {
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /**
     * Return the {@link Instant} that represents the current moment according to {@link Time}.
     *
     * <p>{@link Time#nanoseconds()} is used to get monotonic timestamps.
     */
    public static Instant monotonicNow(final Time time) {
        final long nowNano = time.nanoseconds();
        return Instant.ofEpochSecond(nowNano / NANOS_PER_SECOND, nowNano % NANOS_PER_SECOND);
    }

    /**
     * Measure the duration of a {@link Callable}.
     */
    public static <V> V measureDurationMs(final Time time, final Callable<V> f, final Consumer<Long> callback) throws Exception {
        final Instant start = TimeUtils.monotonicNow(time);
        try {
            return f.call();
        } finally {
            final Instant now = TimeUtils.monotonicNow(time);
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
