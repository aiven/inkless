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

package org.apache.kafka.image.writer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.image.FakeSnapshotWriter;
import org.apache.kafka.metadata.InitDisklessLogFields;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.snapshot.MockRawSnapshotWriter;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.metadata.RecordTestUtils.testRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class RaftSnapshotWriterTest {
    @Test
    public void testFreezeAndClose() {
        FakeSnapshotWriter snapshotWriter = new FakeSnapshotWriter();
        RaftSnapshotWriter writer = new RaftSnapshotWriter(snapshotWriter, 2);
        writer.write(testRecord(0));
        writer.write(testRecord(1));
        writer.write(testRecord(2));
        writer.close(true);
        assertTrue(snapshotWriter.isFrozen());
        assertTrue(snapshotWriter.isClosed());
        assertEquals(List.of(
                List.of(testRecord(0), testRecord(1)),
                List.of(testRecord(2))), snapshotWriter.batches());
    }

    @Test
    public void testCloseWithoutFreeze() {
        FakeSnapshotWriter snapshotWriter = new FakeSnapshotWriter();
        RaftSnapshotWriter writer = new RaftSnapshotWriter(snapshotWriter, 2);
        writer.write(testRecord(0));
        writer.close();
        assertFalse(snapshotWriter.isFrozen());
        assertTrue(snapshotWriter.isClosed());
        assertEquals(List.of(), snapshotWriter.batches());
    }

    @Test
    public void testSplitsBatchesBySerializedSize() {
        FakeSnapshotWriter snapshotWriter = new FakeSnapshotWriter();
        int recordBytes = MetadataRecordSerde.INSTANCE.recordSize(testRecord(0), new ObjectSerializationCache());
        ApiMessageAndVersion largeRecord = new ApiMessageAndVersion(
            new TopicRecord().setName("x".repeat(3 * recordBytes)).setTopicId(Uuid.randomUuid()), (short) 0);

        // Ten records fit in a batch by count, and two test records fit by size.
        RaftSnapshotWriter writer = new RaftSnapshotWriter(snapshotWriter, 10, 2 * recordBytes);
        writer.write(testRecord(0));
        writer.write(testRecord(1));
        writer.write(testRecord(2));
        writer.write(largeRecord);
        writer.write(testRecord(3));
        writer.close(true);

        assertEquals(List.of(
                List.of(testRecord(0), testRecord(1)),
                List.of(testRecord(2)),
                List.of(largeRecord),
                List.of(testRecord(3))), snapshotWriter.batches());
    }

    private static ApiMessageAndVersion switchedPartitionRecord(Uuid topicId, int partitionId, int producerStateEntries) {
        List<InitDisklessLogFields.ProducerStateEntry> producerStates = new ArrayList<>(producerStateEntries);
        for (int i = 0; i < producerStateEntries; i++) {
            producerStates.add(new InitDisklessLogFields.ProducerStateEntry(i, (short) 0, 0, 0, i, 1000L));
        }
        PartitionRecord record = new PartitionRecord().setTopicId(topicId).setPartitionId(partitionId);
        record.unknownTaggedFields().add(InitDisklessLogFields.encodeClassicToDisklessStartOffset(100L));
        record.unknownTaggedFields().add(InitDisklessLogFields.encodeProducerStates(producerStates));
        return new ApiMessageAndVersion(record, (short) 0);
    }

    private static RecordsSnapshotWriter<ApiMessageAndVersion> raftSnapshotWriter(AtomicInteger frozenBytes) {
        return new RecordsSnapshotWriter.Builder()
            .setMaxBatchSize(KafkaRaftClient.MAX_BATCH_SIZE_BYTES)
            .setRawSnapshotWriter(new MockRawSnapshotWriter(
                new OffsetAndEpoch(100L, 10), buffer -> frozenBytes.set(buffer.remaining())))
            .build(MetadataRecordSerde.INSTANCE);
    }

    @Test
    public void testSwitchedPartitionsWithLargeProducerStatesFitInRaftSnapshotBatches() {
        // Three switched partitions of one topic, each with as many producer state entries as one
        // InitDisklessLog request can commit. An image writes them one after the other.
        Uuid topicId = Uuid.randomUuid();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(testRecord(0));
        for (int partitionId = 0; partitionId < 3; partitionId++) {
            records.add(switchedPartitionRecord(topicId, partitionId, InitDisklessLogFields.MAX_PRODUCER_STATE_ENTRIES));
        }
        records.add(testRecord(1));

        // The Raft snapshot writer rejects them as one batch, which is what a batch of 1024 records was.
        try (RecordsSnapshotWriter<ApiMessageAndVersion> oneBatch = raftSnapshotWriter(new AtomicInteger())) {
            assertThrows(RecordBatchTooLargeException.class, () -> oneBatch.append(records));
        }

        AtomicInteger frozenBytes = new AtomicInteger();
        RecordsSnapshotWriter<ApiMessageAndVersion> raftWriter = raftSnapshotWriter(frozenBytes);
        RaftSnapshotWriter writer = new RaftSnapshotWriter(raftWriter, 1024);
        records.forEach(writer::write);
        writer.close(true);

        assertTrue(raftWriter.isFrozen());
        int producerStateBytes = 3 * InitDisklessLogFields.MAX_PRODUCER_STATE_ENTRIES * 34;
        assertTrue(frozenBytes.get() > producerStateBytes, "frozen snapshot has " + frozenBytes.get() + " bytes");
    }
}
