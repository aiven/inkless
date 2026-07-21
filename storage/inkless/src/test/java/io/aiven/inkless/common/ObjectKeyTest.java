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
package io.aiven.inkless.common;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObjectKeyTest {
    @ParameterizedTest
    @CsvSource({
        "true, <prefix>/name",
        "false, realPrefix/name"
    })
    void testCreator(boolean masked, String printed) {
        ObjectKeyCreator objectKeyCreator = ObjectKey.creator("realPrefix", masked);
        final ObjectKey objectKey = objectKeyCreator.create("name");
        assertThat(objectKey.value()).isEqualTo("realPrefix/name");
        assertThat(objectKey.toString()).isEqualTo(printed);
        assertThat(objectKey).isEqualTo(objectKeyCreator.from("realPrefix/name"));
    }

    @ParameterizedTest
    @CsvSource({
        "prefix/,name", // prefix cannot end with a separator
        "/prefix,name", // prefix cannot start with a separator
        "prefix,/name", // name cannot start with a separator
        "prefix/other,na/me" // name cannot contain a separator
    })
    void testCreate_InvalidPath(String prefix, String name) {
        assertThatThrownBy(() -> ObjectKey.Path.create(prefix, name))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({
        "name,'',name",
        "prefix/name,prefix,name",
        "prefix/name/other,prefix/name,other",
        "prefix/name/other/and/so/on,prefix/name/other/and/so,on"
    })
    void testFrom_ValidPath(String value, String prefix, String name) {
        final ObjectKey.Path path = ObjectKey.Path.from(value);
        assertThat(path.prefix()).isEqualTo(prefix);
        assertThat(path.name()).isEqualTo(name);
    }
}
