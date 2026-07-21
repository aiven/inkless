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
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidUrlTest {

    private final ConfigDef.Validator urlValidator = new ValidUrl();

    @Test
    void invalidScheme() {
        assertThatThrownBy(() -> urlValidator.ensureValid("test", "ftp://host")).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value ftp://host for configuration test: "
                + "URL must have scheme from the list [http, https]");
    }

    @Test
    void invalidHost() {
        assertThatThrownBy(() -> urlValidator.ensureValid("test", "host")).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value host for configuration test: Must be a valid URL");
    }

    @Test
    void nullIsValid() {
        assertThatNoException().isThrownBy(() -> urlValidator.ensureValid("test", null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "https"})
    void validSchemes(final String scheme) {
        assertThatNoException().isThrownBy(() -> urlValidator.ensureValid("test", scheme + "://host"));
    }
}
