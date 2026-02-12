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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.share.SharePartitionKey;

import java.util.Objects;

/**
 * This key is used to uniquely identify a cluster mirror by its name.
 */
public record MirrorRecordKey(String mirrorName, Uuid topicId, int partition) {
    public MirrorRecordKey(String mirrorName, Uuid topicId, int partition) {
        this.mirrorName = Objects.requireNonNull(mirrorName, "Mirror name cannot be null");
        this.topicId = Objects.requireNonNull(topicId, "topicId cannot be null");
        this.partition = Objects.requireNonNull(partition, "partition cannot be null");
    }

    public static MirrorRecordKey getInstance(String key) {
        validate(key);
        String[] tokens = key.split(":");
        return new MirrorRecordKey(
                tokens[0].trim(),
                Uuid.fromString(tokens[1]),
                Integer.parseInt(tokens[2])
        );
    }

    public String asCoordinatorKey() {
        return asCoordinatorKey(mirrorName, topicId, partition);
    }

    public static String asCoordinatorKey(String mirrorName, Uuid topicId, int partition) {
        return String.format("%s:%s:%d", mirrorName, topicId, partition);
    }

    public static void validate(String key) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Mirror key cannot be empty");
        }

        String[] tokens = key.split(":");
        if (tokens.length != 3) {
            throw new IllegalArgumentException("Invalid key format: expected - mirrorName:topic:partition, found -  " + key);
        }

        if (tokens[0].trim().isEmpty()) {
            throw new IllegalArgumentException("mirror name must be alphanumeric string");
        }

        try {
            Uuid.fromString(tokens[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid topic ID: " + tokens[1], e);
        }

        try {
            Integer.parseInt(tokens[2]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid partition: " + tokens[2], e);
        }
    }
}
