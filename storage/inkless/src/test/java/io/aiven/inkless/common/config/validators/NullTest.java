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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NullTest {
    @Test
    void nullIsValid() {
        assertThatNoException().isThrownBy(() -> Null.or(ConfigDef.Range.between(1, 2)).ensureValid("test", null));
    }

    @Test
    void nonNullCorrectValueIsValid() {
        assertThatNoException().isThrownBy(() -> Null.or(ConfigDef.Range.between(1, 2)).ensureValid("test", 1));
    }

    @Test
    void invalidValue() {
        assertThatThrownBy(() -> Null.or(ConfigDef.Range.between(1, 2)).ensureValid("test", 5))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 5 for configuration test: Value must be no more than 2");
    }

    @Test
    void testToString() {
        assertThat(Null.or(ConfigDef.Range.between(1, 2)).toString())
            .isEqualTo("null or [1,...,2]");
    }
}
