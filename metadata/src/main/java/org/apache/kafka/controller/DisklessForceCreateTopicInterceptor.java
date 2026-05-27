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
 * A {@link CreateTopicConfigInterceptor} that forces {@code diskless.enable=true} on topics
 * whose names match at least one regex in the configured allow list.
 *
 * <p>Excluded from forcing:
 * <ul>
 *   <li>System topics (as determined by {@link ReplicationControlManager#isSystemTopic})</li>
 *   <li>Topics with a cleanup policy containing "compact"</li>
 * </ul>
 *
 * <p>Controlled by the {@code diskless.force.enable} config, with the allow list
 * specified via {@code diskless.force.include.topic.regexes}.
 */
final class DisklessForceCreateTopicInterceptor implements CreateTopicConfigInterceptor {
    private final List<Pattern> includeTopicPatterns;

    /**
     * @param includeTopicRegexes list of regexes; topics matching at least one are forced to diskless.
     */
    DisklessForceCreateTopicInterceptor(final List<String> includeTopicRegexes) {
        this.includeTopicPatterns = includeTopicRegexes.stream().map(Pattern::compile).toList();
    }

    @Override
    public boolean intercept(
        final String topicName,
        final Map<String, String> requestConfigs,
        final Map<String, Entry<OpType, String>> targetConfigOps
    ) {
        if (shouldForceDisklessEnable(topicName, requestConfigs)) {
            targetConfigOps.put(TopicConfig.DISKLESS_ENABLE_CONFIG, new SimpleImmutableEntry<>(SET, "true"));
            return true;
        }
        return false;
    }

    @Override
    public boolean intercept(
        final String topicName,
        final Map<String, String> targetConfigs
    ) {
        if (shouldForceDisklessEnable(topicName, targetConfigs)) {
            targetConfigs.put(TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
            return true;
        }
        return false;
    }

    private boolean shouldForceDisklessEnable(
        final String topicName,
        final Map<String, String> topicConfigs
    ) {
        if (topicName.startsWith("__")) {
            return false;
        }
        if (ReplicationControlManager.isSystemTopic(topicName)) {
            return false;
        }
        if (cleanupPolicyContainsCompact(topicConfigs)) {
            return false;
        }
        return topicMatchesIncludeRegex(topicName);
    }

    private boolean topicMatchesIncludeRegex(final String topicName) {
        for (Pattern pattern : includeTopicPatterns) {
            if (pattern.matcher(topicName).matches()) {
                return true;
            }
        }
        return false;
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

}
