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

package org.apache.kafka.metadata;

import org.apache.kafka.common.protocol.types.RawTaggedField;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for encoding/decoding Inkless-specific fields for classic-diskless migrations
 * as {@link RawTaggedField}s on standard Kafka metadata records (PartitionRecord, PartitionChangeRecord).
 * High tag numbers (100+) are used to avoid conflicts with upstream Apache Kafka
 * tagged fields. Apache Kafka will silently skip these fields.
 */
public final class InitDisklessLogFields {

    public static final int DISKLESS_START_OFFSET_TAG = 100;
    public static final int PRODUCER_STATES_TAG = 101;

    private InitDisklessLogFields() {}

    // --- disklessStartOffset (tag 100): a single int64 ---

    public static RawTaggedField encodeDisklessStartOffset(long disklessStartOffset) {
        byte[] data = new byte[8];
        ByteBuffer.wrap(data).putLong(disklessStartOffset);
        return new RawTaggedField(DISKLESS_START_OFFSET_TAG, data);
    }

    public static long decodeDisklessStartOffset(List<RawTaggedField> taggedFields) {
        for (RawTaggedField field : taggedFields) {
            if (field.tag() == DISKLESS_START_OFFSET_TAG) {
                return ByteBuffer.wrap(field.data()).getLong();
            }
        }
        return PartitionRegistration.NO_DISKLESS_START_OFFSET;
    }

    // --- producerStates (tag 101): count + fixed-size entries ---

    public record ProducerStateEntry(
        long producerId,
        short producerEpoch,
        int baseSequence,
        int lastSequence,
        long assignedOffset,
        long batchMaxTimestamp
    ) {
        static final int SERIALIZED_SIZE = Long.BYTES + Short.BYTES + Integer.BYTES +
            Integer.BYTES + Long.BYTES + Long.BYTES; // 34 bytes
    }

    public static RawTaggedField encodeProducerStates(List<ProducerStateEntry> states) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + states.size() * ProducerStateEntry.SERIALIZED_SIZE);
        buf.putInt(states.size());
        for (ProducerStateEntry s : states) {
            buf.putLong(s.producerId());
            buf.putShort(s.producerEpoch());
            buf.putInt(s.baseSequence());
            buf.putInt(s.lastSequence());
            buf.putLong(s.assignedOffset());
            buf.putLong(s.batchMaxTimestamp());
        }
        buf.flip();
        return new RawTaggedField(PRODUCER_STATES_TAG, buf.array());
    }

    public static List<ProducerStateEntry> decodeProducerStates(List<RawTaggedField> taggedFields) {
        for (RawTaggedField field : taggedFields) {
            if (field.tag() == PRODUCER_STATES_TAG) {
                ByteBuffer buf = ByteBuffer.wrap(field.data());
                int count = buf.getInt();
                List<ProducerStateEntry> entries = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    entries.add(new ProducerStateEntry(
                        buf.getLong(),
                        buf.getShort(),
                        buf.getInt(),
                        buf.getInt(),
                        buf.getLong(),
                        buf.getLong()
                    ));
                }
                return entries;
            }
        }
        return List.of();
    }
}
