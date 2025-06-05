package io.aiven.inkless.cache;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class ManualClock extends Clock {
    private long currentMillis;
    private final ZoneId zone;

    public ManualClock(Instant instant, ZoneId zone) {
        this.currentMillis = instant.toEpochMilli();
        this.zone = zone;
    }

    public ManualClock() {
        this(Instant.ofEpochMilli(0), ZoneOffset.UTC);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new ManualClock(Instant.ofEpochMilli(currentMillis), zone);
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(currentMillis);
    }

    @Override
    public long millis() {
        return currentMillis;
    }

    public void advanceBy(Duration duration) {
        this.currentMillis += duration.toMillis();
    }
}
