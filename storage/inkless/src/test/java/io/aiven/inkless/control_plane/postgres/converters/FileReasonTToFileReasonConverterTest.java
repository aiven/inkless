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
package io.aiven.inkless.control_plane.postgres.converters;


import org.jooq.generated.enums.FileReasonT;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import io.aiven.inkless.control_plane.FileReason;

import static org.assertj.core.api.Assertions.assertThat;

class FileReasonTToFileReasonConverterTest {
    @ParameterizedTest
    @MethodSource("testArguments")
    void test(final FileReasonT dbObject, final FileReason userObject) {
        final var converter = new FileReasonTToFileReasonConverter();
        assertThat(converter.from(dbObject)).isEqualTo(userObject);
        assertThat(converter.to(userObject)).isEqualTo(dbObject);
    }

    private static Stream<Arguments> testArguments() {
        return Stream.of(
            Arguments.of(null, null),
            Arguments.of(FileReasonT.produce, FileReason.PRODUCE),
            Arguments.of(FileReasonT.merge, FileReason.MERGE)
        );
    }
}
