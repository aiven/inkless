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
package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.internals.Topic;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;

final class TopicCreateRemoteStoragePolicy {
    private final boolean enabled;
    private final Optional<Pattern> localOnlyTopicPattern;

    TopicCreateRemoteStoragePolicy(boolean enabled) {
        this(enabled, "");
    }

    TopicCreateRemoteStoragePolicy(boolean enabled, String localOnlyTopicRegex) {
        this.enabled = enabled;
        this.localOnlyTopicPattern = compilePattern(localOnlyTopicRegex);
    }

    Map<String, Entry<OpType, String>> apply(
        String topicName,
        Map<String, Entry<OpType, String>> keyToOps,
        boolean disklessEnabled
    ) {
        if (!enabled) {
            return keyToOps;
        }

        final boolean remoteStorageEnabled = !disklessEnabled && !isLocalOnlyTopic(topicName);
        Map<String, Entry<OpType, String>> normalized = keyToOps == null ? new HashMap<>() : new HashMap<>(keyToOps);
        normalized.put(REMOTE_LOG_STORAGE_ENABLE_CONFIG, new SimpleImmutableEntry<>(SET, String.valueOf(remoteStorageEnabled)));
        return normalized;
    }

    private boolean isLocalOnlyTopic(String topicName) {
        if (Topic.isInternal(topicName) || topicName.startsWith("__")) {
            return true;
        }
        return localOnlyTopicPattern
            .map(pattern -> pattern.matcher(topicName).matches())
            .orElse(false);
    }

    private static Optional<Pattern> compilePattern(String localOnlyTopicRegex) {
        if (localOnlyTopicRegex == null || localOnlyTopicRegex.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Pattern.compile(localOnlyTopicRegex));
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException(
                "Invalid regex in default remote storage local-only topic pattern: " + localOnlyTopicRegex,
                e
            );
        }
    }
}
