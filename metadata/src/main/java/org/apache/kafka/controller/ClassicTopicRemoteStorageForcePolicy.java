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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_NAME;

final class ClassicTopicRemoteStorageForcePolicy {
    private final boolean enabled;
    private final List<Pattern> excludeTopicPatterns;
    private static final Set<String>
        ADDITIONAL_INTERNAL_TOPICS = Set.of(CLUSTER_METADATA_TOPIC_NAME, "__remote_log_metadata");


    ClassicTopicRemoteStorageForcePolicy(final boolean enabled, final List<String> excludeTopicRegexes) {
        this.enabled = enabled;
        this.excludeTopicPatterns = excludeTopicRegexes.stream().map(Pattern::compile).toList();
    }

    void maybeForceRemoteStorageEnable(
        final String topicName,
        final boolean disklessEnabled,
        final Map<String, String> requestConfigs,
        final Map<String, Entry<OpType, String>> targetConfigOps
    ) {
        if (!enabled) {
            return;
        }
        if (shouldForceRemoteStorageEnable(topicName, disklessEnabled, requestConfigs)) {
            targetConfigOps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, new SimpleImmutableEntry<>(SET, "true"));
        }
    }

    void maybeForceRemoteStorageEnable(
        final String topicName,
        final boolean disklessEnabled,
        final Map<String, String> targetConfigs
    ) {
        if (!enabled) {
            return;
        }
        if (shouldForceRemoteStorageEnable(topicName, disklessEnabled, targetConfigs)) {
            targetConfigs.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
        }
    }

    private boolean shouldForceRemoteStorageEnable(
        final String topicName,
        final boolean disklessEnabled,
        final Map<String, String> topicConfigs
    ) {
        return !(disklessEnabled
            || isInternalTopic(topicName)
            || topicExcludedByRegex(topicName)
            || cleanupPolicyContainsCompact(topicConfigs));
    }

    private boolean isInternalTopic(final String topicName) {
        if (Topic.isInternal(topicName)) {
            return true;
        } else return ADDITIONAL_INTERNAL_TOPICS.contains(topicName);
    }

    private boolean cleanupPolicyContainsCompact(final Map<String, String> topicConfigs) {
        final String cleanupPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG);
        if (cleanupPolicy == null || cleanupPolicy.isEmpty()) {
            return false;
        }
        for (String policy : cleanupPolicy.split(",")) {
            if (TopicConfig.CLEANUP_POLICY_COMPACT.equals(policy.trim())) {
                return true;
            }
        }
        return false;
    }

    private boolean topicExcludedByRegex(final String topicName) {
        for (Pattern pattern : excludeTopicPatterns) {
            if (pattern.matcher(topicName).matches()) {
                return true;
            }
        }
        return false;
    }
}
