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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProcessorRecordContextTest {
    // timestamp + offset + partition: 8 + 8 + 4
    private static final long MIN_SIZE = 20L;

    @Test
    public void shouldNotAllowNullHeaders() {
        assertThrows(
            NullPointerException.class,
            () -> new ProcessorRecordContext(
                42L,
                73L,
                0,
                "topic",
                null
            )
        );
    }

    @Test
    public void shouldEstimateNullTopicAndEmptyHeadersAsZeroLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateEmptyHeaderAsZeroLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateTopicLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE + 5L, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateHeadersLength() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes());
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        assertEquals(MIN_SIZE + 10L + 12L, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateNullValueInHeaderAsZero() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", null);
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        assertEquals(MIN_SIZE + 10L, context.residentMemorySizeEstimate());
    }
}
