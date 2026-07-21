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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;

/**
 * A {@link CreateTopicConfigInterceptor} that forces {@code remote.storage.enable=true} on
 * classic (non-diskless) topics, unless the topic is excluded by cleanup policy, regex, or
 * system topic rules.
 *
 * <p>Controlled by the {@code classic.remote.storage.force.enable} config.
 */
final class ClassicTopicRemoteStorageForceCreateTopicInterceptor implements CreateTopicConfigInterceptor {
    private final List<Pattern> excludeTopicPatterns;
    private final boolean defaultDisklessEnable;

    ClassicTopicRemoteStorageForceCreateTopicInterceptor(final List<String> excludeTopicRegexes, final boolean defaultDisklessEnable) {
        this.excludeTopicPatterns = excludeTopicRegexes.stream().map(Pattern::compile).toList();
        this.defaultDisklessEnable = defaultDisklessEnable;
    }

    @Override
    public boolean intercept(
        final String topicName,
        final Map<String, String> requestConfigs,
        final Map<String, Entry<OpType, String>> targetConfigOps
    ) {
        if (shouldForceRemoteStorageEnable(topicName, requestConfigs)) {
            targetConfigOps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, new SimpleImmutableEntry<>(SET, "true"));
            return true;
        }
        return false;
    }

    @Override
    public boolean intercept(
        final String topicName,
        final Map<String, String> targetConfigs
    ) {
        if (shouldForceRemoteStorageEnable(topicName, targetConfigs)) {
            targetConfigs.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
            return true;
        }
        return false;
    }

    private boolean shouldForceRemoteStorageEnable(
        final String topicName,
        final Map<String, String> topicConfigs
    ) {
        return !(disklessEnabled(topicConfigs)
            || ReplicationControlManager.isSystemTopic(topicName)
            || topicExcludedByRegex(topicName)
            || cleanupPolicyContainsCompact(topicConfigs));
    }

    private boolean disklessEnabled(final Map<String, String> topicConfigs) {
        final String disklessEnableConfigValue = topicConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG);
        if (disklessEnableConfigValue != null) {
            return Boolean.parseBoolean(disklessEnableConfigValue);
        }
        return defaultDisklessEnable;
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
