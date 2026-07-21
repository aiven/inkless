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


import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetDateTimeToInstantConverterTest {
    @ParameterizedTest
    @MethodSource("testArguments")
    void test(final OffsetDateTime offsetDateTime, final Instant instant) {
        final var converter = new OffsetDateTimeToInstantConverter();
        assertThat(converter.from(offsetDateTime)).isEqualTo(instant);
        assertThat(converter.to(instant)).isEqualTo(offsetDateTime);
    }

    private static Stream<Arguments> testArguments() {
        return Stream.of(
            Arguments.of(null, null),
            Arguments.of(
                OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
                Instant.EPOCH),
            Arguments.of(
                OffsetDateTime.of(2025, 1, 15, 15, 24, 42, 213, ZoneOffset.UTC),
                ZonedDateTime.of(2025, 1, 15, 15, 24, 42, 213, ZoneOffset.UTC).toInstant())
        );
    }
}
