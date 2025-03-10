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
package io.aiven.inkless.cache;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;

import static org.assertj.core.api.Assertions.assertThat;

public class FixedBlockAlignmentTest {

    KeyAlignmentStrategy maxBlockSize = new FixedBlockAlignment(Integer.MAX_VALUE);
    KeyAlignmentStrategy kilobyteBlockSize = new FixedBlockAlignment(1000);

    @Test
    public void testNullAndEmptyArguments() {
        assertThat(maxBlockSize.align(null)).isEmpty();
        assertThat(maxBlockSize.align(Collections.emptyList())).isEmpty();
    }

    @Test
    public void testRangeAtStart() {
        assertRanges(maxBlockSize, List.of(
                new ByteRange(0, 10)
        ), Set.of(
                new ByteRange(0, Integer.MAX_VALUE)
        ));
        assertRanges(kilobyteBlockSize, List.of(
                new ByteRange(0, 10)
        ), Set.of(
                new ByteRange(0, 1000)
        ));
    }

    @Test
    public void testRangePastMaxInt() {
        assertRanges(maxBlockSize, List.of(
                new ByteRange(Integer.MAX_VALUE + 10L, 10)
        ), Set.of(
                new ByteRange(Integer.MAX_VALUE, Integer.MAX_VALUE)
        ));
        assertRanges(kilobyteBlockSize, List.of(
                new ByteRange(Integer.MAX_VALUE + 10L, 10)
        ), Set.of(
                new ByteRange(Integer.MAX_VALUE - (Integer.MAX_VALUE % 1000), 1000)
        ));
    }

    @Test
    public void testRangeOverMultipleBlocks() {
        assertRanges(maxBlockSize, List.of(
                new ByteRange(Integer.MAX_VALUE - 10L, 20)
        ), Set.of(
                new ByteRange(0, Integer.MAX_VALUE),
                new ByteRange(Integer.MAX_VALUE, Integer.MAX_VALUE)
        ));
        assertRanges(kilobyteBlockSize, List.of(
                new ByteRange(990, 1020)
        ), Set.of(
                new ByteRange(0, 1000),
                new ByteRange(1000, 1000),
                new ByteRange(2000, 1000)
        ));
    }

    public void assertRanges(KeyAlignmentStrategy strategy, List<ByteRange> input, Set<ByteRange> expectedOutput) {
        Set<ByteRange> actualOutput = strategy.align(input);
        assertThat(actualOutput).isEqualTo(expectedOutput);
    }
}
