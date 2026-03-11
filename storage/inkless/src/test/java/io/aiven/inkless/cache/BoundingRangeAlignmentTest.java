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
package io.aiven.inkless.cache;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;

import static org.assertj.core.api.Assertions.assertThat;

class BoundingRangeAlignmentTest {

    final KeyAlignmentStrategy strategy = new BoundingRangeAlignment();

    @Test
    void nullInput() {
        assertThat(strategy.align(null)).isEmpty();
    }

    @Test
    void emptyInput() {
        assertThat(strategy.align(Collections.emptyList())).isEmpty();
    }

    @Test
    void singleRange() {
        assertThat(strategy.align(List.of(new ByteRange(10, 20))))
            .isEqualTo(Set.of(new ByteRange(10, 20)));
    }

    @Test
    void nonOverlappingRanges() {
        // [0,10) and [20,30) — bounding range is [0,30)
        assertThat(strategy.align(List.of(
            new ByteRange(0, 10),
            new ByteRange(20, 10)
        ))).isEqualTo(Set.of(new ByteRange(0, 30)));
    }

    @Test
    void overlappingRanges() {
        // [0,15) and [10,10) — bounding range is [0,20)
        assertThat(strategy.align(List.of(
            new ByteRange(0, 15),
            new ByteRange(10, 10)
        ))).isEqualTo(Set.of(new ByteRange(0, 20)));
    }

    @Test
    void unsortedInput() {
        // Reverse order: [100,150) before [0,20) — bounding range is [0,150)
        assertThat(strategy.align(List.of(
            new ByteRange(100, 50),
            new ByteRange(0, 20)
        ))).isEqualTo(Set.of(new ByteRange(0, 150)));
    }
}
