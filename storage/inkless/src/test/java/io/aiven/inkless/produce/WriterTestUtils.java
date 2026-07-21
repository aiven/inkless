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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import java.util.HashMap;
import java.util.Map;

public class WriterTestUtils {
    public static class RecordCreator {
        final Map<TopicPartition, Integer> counters = new HashMap<>();

        public MemoryRecords create(final TopicPartition tp, int count) {
            counters.putIfAbsent(tp, 0);

            final int start = counters.get(tp);
            counters.put(tp, start + count);

            final SimpleRecord[] records = new SimpleRecord[count];
            for (int i = 0; i < count; i++) {
                final int number = i + start;
                final byte[] value = String.format("value-%s-%d", tp, number).getBytes();
                final byte[] key = String.format("key-%s-%d", tp, number).getBytes();
                records[i] = new SimpleRecord(0L, value, key, new Header[0]);
            }

            return MemoryRecords.withRecords(Compression.NONE, records);
        }
    }
}
