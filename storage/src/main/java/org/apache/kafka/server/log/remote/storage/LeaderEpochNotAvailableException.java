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

import org.apache.kafka.common.errors.OffsetOutOfRangeException;

/**
 * Thrown when the leader epoch cache of the local log has no leader epoch for the offset of a remote
 * read. The read is out of range for this replica only until a tier state rebuild restores the cache.
 * A read with a known epoch but no remote segment throws a plain {@link OffsetOutOfRangeException}.
 */
public class LeaderEpochNotAvailableException extends OffsetOutOfRangeException {
    private static final long serialVersionUID = 1L;

    public LeaderEpochNotAvailableException(String message) {
        super(message);
    }
}
