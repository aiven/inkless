/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TimeUtilsTest {
    @Mock
    Time time;

    @ParameterizedTest
    @MethodSource("nowParams")
    void now(final long millis, final Instant expectedResult) {
        when(time.milliseconds()).thenReturn(millis);

        final Instant result = TimeUtils.now(time);

        assertThat(result).isEqualTo(expectedResult);
    }

    private static Stream<Arguments> nowParams() {
        return Stream.of(
            Arguments.of(0L, Instant.EPOCH),
            Arguments.of(123L, Instant.EPOCH.plusMillis(123L)),
            Arguments.of(1000L, Instant.ofEpochSecond(1))
        );
    }

    @ParameterizedTest
    @MethodSource("durationMeasurementNowParams")
    void durationMeasurementNow(final long nanos, final Instant expectedResult) {
        when(time.nanoseconds()).thenReturn(nanos);

        final Instant result = TimeUtils.durationMeasurementNow(time);

        assertThat(result).isEqualTo(expectedResult);
    }

    private static Stream<Arguments> durationMeasurementNowParams() {
        return Stream.of(
            Arguments.of(0L, Instant.EPOCH),
            Arguments.of(123L, Instant.EPOCH.plusNanos(123L)),
            Arguments.of(1_000_000L, Instant.ofEpochMilli(1)),
            Arguments.of(10_000_000L, Instant.ofEpochMilli(10)),
            Arguments.of(100_000_000L, Instant.ofEpochMilli(100)),
            Arguments.of(1_000_000_000L, Instant.ofEpochSecond(1))
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void measureDurationMsCallable() throws Exception {
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final Callable<String> callable = () -> "test";
        final Consumer<Long> callback = mock(Consumer.class);
        final var r = TimeUtils.measureDurationMs(time, callable, callback);

        assertThat(r).isEqualTo("test");
        verify(callback).accept(eq(10L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void measureDurationMsCallableException() throws Exception {
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final Exception exception = new Exception("test");
        final Callable<String> callable = () -> {
            throw exception;
        };
        final Consumer<Long> callback = mock(Consumer.class);

        assertThatThrownBy(() -> TimeUtils.measureDurationMs(time, callable, callback))
            .isSameAs(exception);
        // The duration is measured anyway.
        verify(callback).accept(eq(10L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void measureDurationMsRunnable() {
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final Runnable runnable = () -> {};
        final Consumer<Long> callback = mock(Consumer.class);
        TimeUtils.measureDurationMs(time, runnable, callback);

        verify(callback).accept(eq(10L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void measureDurationMsRunnableException() {
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final RuntimeException exception = new RuntimeException("test");
        final Runnable runnable = () -> {
            throw exception;
        };
        final Consumer<Long> callback = mock(Consumer.class);

        assertThatThrownBy(() -> TimeUtils.measureDurationMs(time, runnable, callback))
            .hasRootCause(exception);
        // The duration is measured anyway.
        verify(callback).accept(eq(10L));
    }
}
