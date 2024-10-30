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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * A description of an ongoing compaction operation
 */
public class CompactionPlan {

    // Manifests of one or more objects which should be read from during compaction
    private final List<CommittedBatchCoordinate> existing;
    // List of expected output objects, and the partitions they should contain
    private final Map<Uuid, List<TopicIdPartition>> distribution;
    // Usage hints to use when writing output objects
    private final Map<Uuid, UsageHint> usageHints;
    // List of objects to delete. Cannot overlap with any other field.
    private final List<Uuid> deletes;
    // How long this operation may take, before it's output should be discarded and the operation is reassigned.
    private final Duration lease;

    public CompactionPlan(List<CommittedBatchCoordinate> existing, Map<Uuid, List<TopicIdPartition>> distribution, Map<Uuid, UsageHint> usageHints, List<Uuid> deletes, Duration lease) {
        this.existing = existing;
        this.distribution = distribution;
        this.usageHints = usageHints;
        this.deletes = deletes;
        this.lease = lease;
    }
}
