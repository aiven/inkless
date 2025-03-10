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
package io.aiven.inkless.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlainObjectKeyTest {
    @Test
    void notAllowNullPrefix() {
        assertThatThrownBy(() -> PlainObjectKey.create(null, "suffix"))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("prefix cannot be null");
    }

    @Test
    void notAllowNullMainPath() {
        assertThatThrownBy(() -> PlainObjectKey.create("prefix", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("name cannot be null");
    }

    @Test
    void value() {
        assertThat(PlainObjectKey.create("prefix", "suffix").value())
            .isEqualTo("prefix/suffix");
    }

    @Test
    void testToString() {
        assertThat(PlainObjectKey.from("prefix/suffix").toString())
            .isEqualTo("prefix/suffix");
    }
}
