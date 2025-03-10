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

class ByteRangeTest {
    @Test
    void negativeOffset() {
        assertThatThrownBy(() -> new ByteRange(-1, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("offset cannot be negative, -1 given");
    }

    @Test
    void negativeSize() {
        assertThatThrownBy(() -> new ByteRange(1, -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("size cannot be negative, -1 given");
    }

    @Test
    void empty() {
        assertThat(new ByteRange(1, 0).empty()).isTrue();
        assertThat(new ByteRange(1, 1).empty()).isFalse();
    }

    @Test
    void endOffset() {
        assertThat(new ByteRange(1, 1).endOffset()).isEqualTo(1);
        assertThat(new ByteRange(1, 2).endOffset()).isEqualTo(2);
    }

    @Test
    void maxRange() {
        assertThat(ByteRange.maxRange()).isEqualTo(new ByteRange(0, Long.MAX_VALUE));
    }
}