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
package kafka.server;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wraps the tiered storage plugin and fails the copy of a segment that ends at or above the offset that a
 * test blocks for its topic. The remote log manager copies segments in order and retries a failed copy, so
 * remote storage then holds the segments below the first blocked one. A test uses it to hold a switching
 * partition in phase 1.
 */
public class GatedRemoteStorageManager implements RemoteStorageManager {
    // The plugin's dependencies aren't on the test compile classpath, so the class is loaded by name.
    private static final String DELEGATE_CLASS_NAME = "io.aiven.kafka.tieredstorage.RemoteStorageManager";
    private static final Map<String, Long> BLOCKED_FROM_OFFSET = new ConcurrentHashMap<>();

    private final RemoteStorageManager delegate;

    public GatedRemoteStorageManager() throws ReflectiveOperationException {
        delegate = (RemoteStorageManager) Class.forName(DELEGATE_CLASS_NAME).getDeclaredConstructor().newInstance();
    }

    static void blockCopies(String topic, long fromOffset) {
        BLOCKED_FROM_OFFSET.put(topic, fromOffset);
    }

    static void reset() {
        BLOCKED_FROM_OFFSET.clear();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        delegate.configure(configs);
    }

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(
        RemoteLogSegmentMetadata metadata,
        LogSegmentData data
    ) throws RemoteStorageException {
        Long blockedFrom = BLOCKED_FROM_OFFSET.get(metadata.topicIdPartition().topic());
        if (blockedFrom != null && metadata.endOffset() >= blockedFrom) {
            throw new RemoteStorageException("The test blocks the copy of " + metadata.remoteLogSegmentId()
                + " because it ends at " + metadata.endOffset() + ", at or above " + blockedFrom);
        }
        return delegate.copyLogSegmentData(metadata, data);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata, int startPosition)
        throws RemoteStorageException {
        return delegate.fetchLogSegment(metadata, startPosition);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata, int startPosition, int endPosition)
        throws RemoteStorageException {
        return delegate.fetchLogSegment(metadata, startPosition, endPosition);
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata metadata, IndexType indexType)
        throws RemoteStorageException {
        return delegate.fetchIndex(metadata, indexType);
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        delegate.deleteLogSegmentData(metadata);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
