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

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class UUIDtoUuidConverterTest {
    @ParameterizedTest
    @MethodSource("testArguments")
    void test(final UUID javaUuid, final Uuid kafkaUuid) {
        final var converter = new UUIDtoUuidConverter();
        assertThat(converter.from(javaUuid)).isEqualTo(kafkaUuid);
        assertThat(converter.to(kafkaUuid)).isEqualTo(javaUuid);
    }

    private static Stream<Arguments> testArguments() {
        return Stream.of(
            Arguments.of(null, null),
            Arguments.of(new UUID(0, 0), Uuid.ZERO_UUID),
            Arguments.of(
                new UUID(123, 456),
                new Uuid(123, 456))
        );
    }
}
