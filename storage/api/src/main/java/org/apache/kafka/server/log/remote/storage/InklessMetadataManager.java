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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Object responsible for storing and retrieving metadata about data stored by the {@link InklessStorageManager}.
 */
public interface InklessMetadataManager extends Configurable, Closeable {

    /**
     * @return UsageHint to use when writing active segments
     */
    UsageHint activeHint();

    /**
     * Commit a group of already-uploaded batches, recording their coordinates for later lookup.
     * This additionally assigns offsets for the batches in a global order.
     * The global order should respect the local order of passed-in batches within this request.
     * This method should be serial, and one call must succeed or fail before the next may be executed.
     * @return The passed in coordinates, with offsets assigned.
     */
    List<CommittedBatchCoordinate> commitBatches(List<BatchCoordinate> coordinates);

    /**
     * Start a compaction operation, obtaining a lease
     * @return A description of a compaction operation that should be performed by this node.
     */
    CompactionPlan startCompaction();

    /**
     * Abort a compaction operation, releasing the lease early
     * @param objects A list of objects that were leased to the object and need compaction via another node.
     */
    void abortCompaction(List<Uuid> objects);

    /**
     * Successfully finish a compaction operation, persisting a new set of batch coordinates.
     * @param coordinates New coordinates to replace existing ones.
     */
    void finishCompaction(List<CommittedBatchCoordinate> coordinates);

    /**
     * @param query For each partition, a range of log offsets within that partition.
     * @return A contiguous list of batches for the specified partitions, potentially on multiple objects.
     */
    List<CommittedBatchCoordinate> findBatches(Map<TopicIdPartition, OffsetRange> query);

    /**
     * @param objects A list of objects which contain batches
     * @return A list of all batches in the specified objects.
     */
    List<CommittedBatchCoordinate> describeObjects(List<Uuid> objects);
}
