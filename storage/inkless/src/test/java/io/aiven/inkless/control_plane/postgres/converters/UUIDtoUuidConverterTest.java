// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
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
