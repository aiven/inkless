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

/**
 * A coordinate for a batch which may not have offsets assigned yet.
 */
public class BatchCoordinate {

    // The object and partition that contain this batch.
    private final RemoteLogSegmentId segmentId;
    // Index into the object where the first byte of this batch is present
    private final long offset;
    // Number of contiguous bytes in the object this batch takes up.
    private final int length;
    // Number of sequence numbers this batch takes up in the log
    private final long sequenceCount;
    // TODO: idempotent/transactional producer metadata

    public BatchCoordinate(RemoteLogSegmentId segmentId, long offset, int length, long sequenceCount) {
        this.segmentId = segmentId;
        this.offset = offset;
        this.length = length;
        this.sequenceCount = sequenceCount;
    }
}
