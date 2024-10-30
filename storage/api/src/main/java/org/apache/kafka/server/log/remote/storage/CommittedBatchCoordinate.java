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
 * Coordinate for a batch which has offsets assigned already.
 */
public class CommittedBatchCoordinate extends BatchCoordinate {

    // Log offset at the start of this batch
    private final long baseOffset;
    // The unix epoch time when this coordinate expires, and the data may no longer be available
    private final long expireTimeMs;

    public CommittedBatchCoordinate(RemoteLogSegmentId segmentId, long offset, int length, long sequenceCount, long baseOffset, long expireTimeMs) {
        super(segmentId, offset, length, sequenceCount);
        this.baseOffset = baseOffset;
        this.expireTimeMs = expireTimeMs;
    }
}
