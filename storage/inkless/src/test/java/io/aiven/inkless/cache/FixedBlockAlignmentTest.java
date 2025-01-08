/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    // TODO INK-77: This behavior should be removed
    @Test
    public void testRangeOverMultipleBlocks() {
        assertRanges(maxBlockSize, List.of(
                new ByteRange(Integer.MAX_VALUE - 10L, 20)
        ), Set.of(
                new ByteRange(Integer.MAX_VALUE - 10L, 20)
        ));
        assertRanges(kilobyteBlockSize, List.of(
                new ByteRange(990, 20)
        ), Set.of(
                new ByteRange(990, 20)
        ));
    }

    public void assertRanges(KeyAlignmentStrategy strategy, List<ByteRange> input, Set<ByteRange> expectedOutput) {
        Set<ByteRange> actualOutput = strategy.align(input);
        assertThat(actualOutput).isEqualTo(expectedOutput);
    }
}
