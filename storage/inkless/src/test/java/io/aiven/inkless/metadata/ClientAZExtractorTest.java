// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

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
        "inkless_az=az1",
        "aaa=bbb,inkless_az=az1",
        "aaa=bbb, inkless_az=az1",
        "inkless_az=az1,aaa=bbb",
        "inkless_az=az1, aaa=bbb"
    })
    void getClientAZWhenProvided(final String clientId) {
        assertThat(ClientAZExtractor.getClientAZ(clientId)).isEqualTo("az1");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "inkless_az=",
        "aaa=bbb,inkless_az=",
        "aaa=bbb, inkless_az=",
        "inkless_az=,aaa=bbb",
        "inkless_az=, aaa=bbb"
    })
    void getClientAZWhenProvidedEmptyValue(final String clientId) {
        assertThat(ClientAZExtractor.getClientAZ(clientId)).isEqualTo("");
    }

    @ParameterizedTest
    @ValueSource(strings = {"aaainkless_az=az1"})
    void getClientAZWhenOnlySeeminglyCorrect(final String clientId) {
        assertThat(ClientAZExtractor.getClientAZ(clientId)).isNull();
    }
}
