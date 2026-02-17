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

import java.util.Arrays;
import java.util.Objects;

/**
 * This key is used to uniquely identify a mirror partition in the coordinator.
 * The composite key format is {@code mirrorName:topicId:partition}.
 *
 * <p>Mirror names must be non-empty, at most 249 characters, contain only ASCII
 * alphanumerics, '.', '_' and '-', and cannot be "." or "..".
 *
 * <p>The key is parsed right-to-left: partition is the last segment, topicId is the
 * second-to-last, and mirrorName is everything before that.
 *
 * @param mirrorName the mirror name
 * @param topicId the topic ID
 * @param partition the partition index
 */
public record MirrorRecordKey(String mirrorName, Uuid topicId, int partition) {
    private static final int MAX_NAME_LENGTH = 249;

    public MirrorRecordKey(String mirrorName, Uuid topicId, int partition) {
        this.mirrorName = Objects.requireNonNull(mirrorName, "Mirror name cannot be null");
        validateMirrorName(mirrorName);
        this.topicId = Objects.requireNonNull(topicId, "topicId cannot be null");
        this.partition = Objects.requireNonNull(partition, "partition cannot be null");
    }

    // TODO these are the same rules used for topic names, so it may be better to extract into some shared utils class
    private static void validateMirrorName(String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Mirror name is invalid: the empty string is not allowed");
        }
        if (".".equals(name)) {
            throw new IllegalArgumentException("Mirror name is invalid: '.' is not allowed");
        }
        if ("..".equals(name)) {
            throw new IllegalArgumentException("Mirror name is invalid: '..' is not allowed");
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("Mirror name is invalid: the length of '" + name +
                    "' is longer than the max allowed length " + MAX_NAME_LENGTH);
        }
        if (!containsValidPattern(name)) {
            throw new IllegalArgumentException("Mirror name is invalid: '" + name +
                    "' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'");
        }
    }

    private static boolean containsValidPattern(String name) {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean isAlphanumeric = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z');
            if (!isAlphanumeric && c != '.' && c != '_' && c != '-') return false;
        }
        return true;
    }

    /**
     * Returns a {@link MirrorRecordKey} parsed from a string of format {@code mirrorName:topicId:partition}.
     * The key is parsed right-to-left to correctly handle mirror names that may contain colons.
     *
     * @param key the string to parse
     * @return the parsed {@link MirrorRecordKey}
     * @throws IllegalArgumentException if the key is empty or has invalid format
     */
    public static MirrorRecordKey getInstance(String key) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Mirror key cannot be empty");
        }

        String[] tokens = key.split(":");
        if (tokens.length < 3) {
            throw new IllegalArgumentException("Invalid key format: expected - mirrorName:topicId:partition, found - " + key);
        }

        int last = tokens.length - 1;
        String partitionStr = tokens[last];
        String topicIdStr = tokens[last - 1];
        String mirrorName = String.join(":", Arrays.copyOfRange(tokens, 0, last - 1));

        if (mirrorName.trim().isEmpty()) {
            throw new IllegalArgumentException("Mirror name must not be empty");
        }

        Uuid topicId;
        try {
            topicId = Uuid.fromString(topicIdStr);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid topic ID: " + topicIdStr, e);
        }

        int partition;
        try {
            partition = Integer.parseInt(partitionStr);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid partition: " + partitionStr, e);
        }

        return new MirrorRecordKey(mirrorName, topicId, partition);
    }

    /**
     * Returns the string representation of this key in the format {@code mirrorName:topicId:partition}.
     *
     * @return the coordinator key string
     */
    public String asCoordinatorKey() {
        return asCoordinatorKey(mirrorName, topicId, partition);
    }

    /**
     * Returns the coordinator key string for the given components.
     *
     * @param mirrorName the mirror name
     * @param topicId the topic ID
     * @param partition the partition index
     * @return the coordinator key string
     */
    public static String asCoordinatorKey(String mirrorName, Uuid topicId, int partition) {
        return String.format("%s:%s:%d", mirrorName, topicId, partition);
    }

    /**
     * Validates whether the string argument has a valid {@link MirrorRecordKey} format.
     *
     * @param key the string to validate
     * @throws IllegalArgumentException if the key is empty or has invalid format
     */
    public static void validate(String key) {
        getInstance(key);
    }
}
