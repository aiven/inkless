// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
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
