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
package io.aiven.inkless.control_plane.postgres.converters;

import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShortToTimestampTypeConverterTest {
    @ParameterizedTest
    @MethodSource("testTimestampTypeArguments")
    void testTimestampType(final Short id, final TimestampType expected) {
        final var converter = new ShortToTimestampTypeConverter();
        assertThat(converter.from(id)).isEqualTo(expected);
        assertThat(converter.to(expected)).isEqualTo(id);
    }

    private static Stream<Arguments> testTimestampTypeArguments() {
        return Stream.of(
            Arguments.of(null, null),
            Arguments.of((short)0, TimestampType.CREATE_TIME),
            Arguments.of((short)1, TimestampType.LOG_APPEND_TIME),
            Arguments.of((short)-1, TimestampType.NO_TIMESTAMP_TYPE)
        );
    }

    @Test
    void unknownTimestampType() {
        final var converter = new ShortToTimestampTypeConverter();
        assertThatThrownBy(() -> converter.from((short) 2))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Unexpected value: 2");
    }
}
