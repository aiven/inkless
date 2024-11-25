// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import java.util.HashMap;
import java.util.Map;

class WriterTestUtils {
    static class RecordCreator {
        final Map<TopicPartition, Integer> counters = new HashMap<>();

        MemoryRecords create(final TopicPartition tp, int count) {
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
