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

import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;


/**
 * Writes out a metadata image to a SnapshotWriter.
 */
public class RaftSnapshotWriter implements ImageWriter {
    // Inkless: the snapshot writer rejects a batch over KafkaRaftClient.MAX_BATCH_SIZE_BYTES. The record of a
    // switched partition has its committed producer states, and a batch of batchSize such records can exceed
    // that size. Half of that size leaves room for the framing of the records.
    static final int DEFAULT_MAX_BATCH_BYTES = KafkaRaftClient.MAX_BATCH_SIZE_BYTES / 2;

    private final SnapshotWriter<ApiMessageAndVersion> snapshotWriter;
    private final int batchSize;
    private final int maxBatchBytes;
    private List<ApiMessageAndVersion> records;
    private long recordsBytes;
    private ObjectSerializationCache serializationCache;
    private OptionalLong frozenSize = OptionalLong.empty();

    public RaftSnapshotWriter(
        SnapshotWriter<ApiMessageAndVersion> snapshotWriter,
        int batchSize
    ) {
        this(snapshotWriter, batchSize, DEFAULT_MAX_BATCH_BYTES);
    }

    RaftSnapshotWriter(
        SnapshotWriter<ApiMessageAndVersion> snapshotWriter,
        int batchSize,
        int maxBatchBytes
    ) {
        this.snapshotWriter = snapshotWriter;
        this.batchSize = batchSize;
        this.maxBatchBytes = maxBatchBytes;
        startBatch();
    }

    @Override
    public void write(ApiMessageAndVersion record) {
        if (records == null) throw new ImageWriterClosedException();
        // Inkless: a record that doesn't fit starts the next batch. A record over the limit goes alone.
        int recordBytes = MetadataRecordSerde.INSTANCE.recordSize(record, serializationCache);
        if (!records.isEmpty() && recordsBytes + recordBytes > maxBatchBytes) {
            appendBatch();
        }
        records.add(record);
        recordsBytes += recordBytes;
        if (records.size() >= batchSize) {
            appendBatch();
        }
    }

    private void appendBatch() {
        snapshotWriter.append(records);
        startBatch();
    }

    private void startBatch() {
        records = new ArrayList<>();
        recordsBytes = 0;
        // The cache keeps a reference to every object it measured, so each batch gets its own.
        serializationCache = new ObjectSerializationCache();
    }

    @Override
    public void close(boolean complete) {
        if (records == null) return;
        try {
            if (complete) {
                if (!records.isEmpty()) {
                    snapshotWriter.append(records);
                }
                frozenSize = OptionalLong.of(snapshotWriter.freeze());
            }
        } finally {
            records = null;
            snapshotWriter.close();
        }
    }

    /**
     * @return the frozen size of the snapshot, or OptionalLong.empty if the snapshot was not frozen.
     */
    public OptionalLong frozenSize() {
        return frozenSize;
    }
}
