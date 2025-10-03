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

    @Test
    void unionWithNulls() {
        assertThatThrownBy(() -> ByteRange.union(null, new ByteRange(1, 2)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("a cannot be null");
        assertThatThrownBy(() -> ByteRange.union(new ByteRange(1, 2), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("b cannot be null");
    }

    @Test
    void unionWithEmptyA() {
        final ByteRange empty = new ByteRange(5, 0);
        final ByteRange nonEmpty = new ByteRange(1, 3);
        assertThat(ByteRange.union(empty, nonEmpty)).isEqualTo(nonEmpty);
    }

    @Test
    void unionWithEmptyB() {
        final ByteRange nonEmpty = new ByteRange(1, 3);
        final ByteRange empty = new ByteRange(5, 0);
        assertThat(ByteRange.union(nonEmpty, empty)).isEqualTo(nonEmpty);
    }

    @Test
    void unionWithBothEmpty() {
        final ByteRange empty1 = new ByteRange(1, 0);
        final ByteRange empty2 = new ByteRange(5, 0);
        assertThat(ByteRange.union(empty1, empty2)).isEqualTo(empty1);
    }

    @Test
    void unionWithOverlappingRanges() {
        final ByteRange a = new ByteRange(1, 5);  // [1..5]
        final ByteRange b = new ByteRange(3, 4);  // [3..6]
        final ByteRange expected = new ByteRange(1, 6);  // [1..6]
        assertThat(ByteRange.union(a, b)).isEqualTo(expected);
    }

    @Test
    void unionWithAdjacentRanges() {
        final ByteRange a = new ByteRange(1, 3);  // [1..3]
        final ByteRange b = new ByteRange(4, 2);  // [4..5]
        final ByteRange expected = new ByteRange(1, 5);  // [1..5]
        assertThat(ByteRange.union(a, b)).isEqualTo(expected);
    }

    @Test
    void unionWithDisjointRanges() {
        final ByteRange a = new ByteRange(1, 2);  // [1..2]
        final ByteRange b = new ByteRange(5, 3);  // [5..7]
        final ByteRange expected = new ByteRange(1, 7);  // [1..7]
        assertThat(ByteRange.union(a, b)).isEqualTo(expected);
    }

    @Test
    void unionWithIdenticalRanges() {
        final ByteRange range = new ByteRange(2, 4);
        assertThat(ByteRange.union(range, range)).isEqualTo(range);
    }

    @Test
    void unionWithOneContainingTheOther() {
        final ByteRange larger = new ByteRange(1, 10);  // [1..10]
        final ByteRange smaller = new ByteRange(3, 4);  // [3..6]
        final ByteRange expected = new ByteRange(1, 10);  // [1..10]
        assertThat(ByteRange.union(larger, smaller)).isEqualTo(expected);
    }
}
