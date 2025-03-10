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
