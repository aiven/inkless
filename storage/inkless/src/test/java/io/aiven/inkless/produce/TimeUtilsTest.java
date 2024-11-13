// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TimeUtilsTest {
    @Test
    void startAfterNowFinishAfterNow() {
        final Duration result = TimeUtils.durationFromNow(Instant.ofEpochMilli(10), Instant.ofEpochMilli(1000), Duration.ofMillis(300));
        assertThat(result).isEqualTo(Duration.ofMillis(300));
    }

    @Test
    void startNowFinishAfterNow() {
        final Duration result = TimeUtils.durationFromNow(Instant.ofEpochMilli(10), Instant.ofEpochMilli(10), Duration.ofMillis(300));
        assertThat(result).isEqualTo(Duration.ofMillis(300));
    }

    @Test
    void startBeforeNowFinishAfterNow() {
        final Duration result = TimeUtils.durationFromNow(Instant.ofEpochMilli(110), Instant.ofEpochMilli(10), Duration.ofMillis(300));
        assertThat(result).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void startNowFinishNow() {
        final Duration result = TimeUtils.durationFromNow(Instant.ofEpochMilli(10), Instant.ofEpochMilli(10), Duration.ofMillis(0));
        assertThat(result).isEqualTo(Duration.ofMillis(0));
    }

    @Test
    void startBeforeNowFinishNow() {
        final Duration result = TimeUtils.durationFromNow(Instant.ofEpochMilli(100), Instant.ofEpochMilli(10), Duration.ofMillis(90));
        assertThat(result).isEqualTo(Duration.ofMillis(0));
    }

    @Test
    void startBeforeNowFinishBeforeNow() {
        final Duration result = TimeUtils.durationFromNow(Instant.ofEpochMilli(1000), Instant.ofEpochMilli(10), Duration.ofMillis(10));
        assertThat(result).isEqualTo(Duration.ofMillis(0));
    }
}
