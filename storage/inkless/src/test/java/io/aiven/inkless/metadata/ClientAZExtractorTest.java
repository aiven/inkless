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
package io.aiven.inkless.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ClientAZExtractorTest {

    @Test
    void getClientAZNullClientId() {
        assertThat(ClientAZExtractor.getClientAZ(null)).isNull();
    }

    @Test
    void getClientAZEmptyClientId() {
        assertThat(ClientAZExtractor.getClientAZ("")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "diskless_az=az1",
        "aaa=bbb,diskless_az=az1",
        "aaa=bbb, diskless_az=az1",
        "diskless_az=az1,aaa=bbb",
        "diskless_az=az1, aaa=bbb"
    })
    void getClientAZWhenProvided(final String clientId) {
        assertThat(ClientAZExtractor.getClientAZ(clientId)).isEqualTo("az1");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "diskless_az=",
        "aaa=bbb,diskless_az=",
        "aaa=bbb, diskless_az=",
        "diskless_az=,aaa=bbb",
        "diskless_az=, aaa=bbb"
    })
    void getClientAZWhenProvidedEmptyValue(final String clientId) {
        assertThat(ClientAZExtractor.getClientAZ(clientId)).isEqualTo("");
    }

    @ParameterizedTest
    @ValueSource(strings = {"aaadiskless_az=az1"})
    void getClientAZWhenOnlySeeminglyCorrect(final String clientId) {
        assertThat(ClientAZExtractor.getClientAZ(clientId)).isNull();
    }

    @Test
    void withoutKnownRacksReturnRawValueIncludingSuffix() {
        assertThat(ClientAZExtractor.getClientAZ(
                "kafka-connect,diskless_az=use1-az2-575969bf-aae2-47c5-98d6-8750ef0536f8"
        )).isEqualTo("use1-az2-575969bf-aae2-47c5-98d6-8750ef0536f8");
    }

    @Test
    void withoutKnownRacksReturnRawValueWithBackingStoreSuffix() {
        assertThat(ClientAZExtractor.getClientAZ(
                "connect-cluster-kafka-connect,diskless_az=use1-az2-configs"
        )).isEqualTo("use1-az2-configs");
    }

    @ParameterizedTest
    @MethodSource("knownRacksCases")
    void getClientAZWithKnownRacks(final String clientId, final Set<String> knownRacks, final String expected) {
        assertThat(ClientAZExtractor.getClientAZ(clientId, () -> knownRacks)).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("noValidationCases")
    void getClientAZFallsBackToRawValueWithoutValidation(final Set<String> knownRacks) {
        assertThat(ClientAZExtractor.getClientAZ("my-app,diskless_az=use1-az2-suffix", () -> knownRacks))
                .isEqualTo("use1-az2-suffix");
    }

    static Stream<Arguments> knownRacksCases() {
        final Set<String> euWest = Set.of("eu-west-1a", "eu-west-1b", "eu-west-1c");
        final Set<String> use1 = Set.of("use1-az2", "use1-az4", "use1-az6");
        return Stream.of(
            arguments("my-app,diskless_az=eu-west-1a", euWest, "eu-west-1a"),
            arguments("diskless_az=az1", Set.of("az0", "az1", "az2"), "az1"),
            arguments("kafka-connect,diskless_az=use1-az2-575969bf-aae2-47c5-98d6-8750ef0536f8", use1, "use1-az2"),
            arguments("connect-cluster-kafka-connect,diskless_az=use1-az2-configs", use1, "use1-az2"),
            arguments("connect-cluster-kafka-connect,diskless_az=eu-west-1a-offsets", euWest, "eu-west-1a"),
            arguments("connect-cluster-kafka-connect,diskless_az=eu-west-1a-statuses", euWest, "eu-west-1a"),
            arguments("my-app,diskless_az=us-east-1a-extra-suffix", Set.of("us-east-1a", "us-east-1b"), "us-east-1a"),
            arguments("kafka-connect,diskless_az=eu-west-1a-deadbeef-1234-5678-abcd-ef0123456789", euWest, "eu-west-1a"),
            arguments("kafka-connect,diskless_az=az1-575969bf-aae2-47c5-98d6-8750ef0536f8", Set.of("az1", "az2", "az3"), "az1"),
            // Progressive stripping removes from the right, so the longest prefix that matches wins.
            arguments("my-app,diskless_az=us-east-1a-suffix", Set.of("us", "us-east", "us-east-1a"), "us-east-1a"),
            arguments("kafka-connect,diskless_az=use1-az2-575969bf-aae2-47c5-98d6-8750ef0536f8", Set.of("use1-az2"), "use1-az2"),
            arguments("my-app,diskless_az=", euWest, ""),
            arguments("my-app,diskless_az=completely-wrong-value", euWest, null),
            arguments("my-app,diskless_az=unknown", euWest, null),
            arguments("my-app,diskless_az=eu-west-kaboom", euWest, null),
            arguments(null, euWest, null),
            arguments("connector-producer-orders-0", euWest, null),
            arguments("connector-producer-orders-source-0", use1, null),
            arguments("connector-consumer-my-sink-0", use1, null),
            arguments("source->target|MirrorSourceConnector-0|replication-consumer", euWest, null)
        );
    }

    static Stream<Arguments> noValidationCases() {
        return Stream.of(
            arguments((Set<String>) null),
            arguments(Set.of())
        );
    }
}
