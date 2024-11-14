// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.time.Instant;

class TimeUtils {
    static Duration durationFromNow(final Instant now, final Instant start, final Duration duration) {
        long millisPassed = Duration.between(start, now).toMillis();
        millisPassed = Math.max(0, millisPassed);

        long remainingMillis = duration.minusMillis(millisPassed).toMillis();
        remainingMillis = Math.max(0, remainingMillis);

        return Duration.ofMillis(remainingMillis);
    }
}
