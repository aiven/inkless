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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ClassicTopicRemoteStorageForceCreateTopicInterceptorTest {
    @Test
    public void forcesRemoteStorageForClassicTopic() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of(), false);
        final String topicName = "classic-topic-force-enabled";
        final Map<String, String> requestConfigs = new HashMap<>();
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptor.intercept(topicName, requestConfigs, targetConfigOps);
        interceptor.intercept(topicName, targetConfigs);

        assertRemoteStorageEnabled(targetConfigOps, targetConfigs);
    }

    @Test
    public void doesNotForceForCompactedTopicCleanupPolicyCompact() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of(), false);
        final String topicName = "compacted-topic-only-compact";
        final Map<String, String> requestConfigs = Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
        );
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>(requestConfigs);

        interceptor.intercept(topicName, requestConfigs, targetConfigOps);
        interceptor.intercept(topicName, targetConfigs);

        assertRemoteStorageDisabled(targetConfigOps, targetConfigs);
    }

    @Test
    public void doesNotForceForCompactedTopicCleanupPolicyDeleteCompact() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of(), false);
        final String topicName = "compacted-topic-delete-compact";
        final Map<String, String> requestConfigs = Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE + ", " + TopicConfig.CLEANUP_POLICY_COMPACT
        );
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>(requestConfigs);

        interceptor.intercept(topicName, requestConfigs, targetConfigOps);
        interceptor.intercept(topicName, targetConfigs);

        assertRemoteStorageDisabled(targetConfigOps, targetConfigs);
    }

    @Test
    public void doesNotForceForCompactedTopicCleanupPolicyCompactDelete() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of(), false);
        final String topicName = "compacted-topic-compact-delete";
        final Map<String, String> requestConfigs = Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE
        );
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>(requestConfigs);

        interceptor.intercept(topicName, requestConfigs, targetConfigOps);
        interceptor.intercept(topicName, targetConfigs);

        assertRemoteStorageDisabled(targetConfigOps, targetConfigs);
    }

    @Test
    public void doesNotForceForExcludedTopicRegex() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of("_schemas", "mm2-(.*)"), false);
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptor.intercept("_schemas", Map.of(), targetConfigOps);
        interceptor.intercept("mm2-heartbeats", targetConfigs);

        assertRemoteStorageDisabled(targetConfigOps, targetConfigs);
    }

    @Test
    public void doesNotForceForCompactedExcludedTopicRegex() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of("_schemas", "mm2-(.*)"), false);
        final String topicName = "_schemas";
        final Map<String, String> compactedConfigs = Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
        );
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>(compactedConfigs);

        interceptor.intercept(topicName, compactedConfigs, targetConfigOps);
        interceptor.intercept(topicName, targetConfigs);

        assertRemoteStorageDisabled(targetConfigOps, targetConfigs);
    }

    @Test
    public void doesNotForceForDisklessTopicExplicit() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of(), false);
        final String topicName = "diskless-topic";
        final Map<String, String> requestConfigs = new HashMap<>(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));

        interceptor.intercept(topicName, requestConfigs, targetConfigOps);
        interceptor.intercept(topicName, targetConfigs);

        assertRemoteStorageDisabled(targetConfigOps, targetConfigs);
    }

    @Test
    public void doesNotForceForDisklessTopicByDefault() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of(), true);
        final String topicName = "diskless-topic-by-default";
        final Map<String, String> requestConfigs = new HashMap<>();
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptor.intercept(topicName, requestConfigs, targetConfigOps);
        interceptor.intercept(topicName, targetConfigs);

        assertRemoteStorageDisabled(targetConfigOps, targetConfigs);
    }

    @Test
    public void doesNotForceForAllExcludedInternalTopics() {
        final var interceptor = new ClassicTopicRemoteStorageForceCreateTopicInterceptor(List.of(), false);
        for (String topicName : List.of(
            Topic.GROUP_METADATA_TOPIC_NAME,
            Topic.TRANSACTION_STATE_TOPIC_NAME,
            Topic.SHARE_GROUP_STATE_TOPIC_NAME,
            Topic.CLUSTER_METADATA_TOPIC_NAME,
            "__remote_log_metadata"
        )) {
            final Map<String, String> requestConfigs = new HashMap<>();
            final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
            final Map<String, String> targetConfigs = new HashMap<>();

            interceptor.intercept(topicName, requestConfigs, targetConfigOps);
            interceptor.intercept(topicName, targetConfigs);

            assertRemoteStorageDisabled(targetConfigOps, targetConfigs);
        }
    }

    private static void assertRemoteStorageEnabled(
        final Map<String, Entry<OpType, String>> targetConfigOps,
        final Map<String, String> targetConfigs
    ) {
        assertEquals(Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, Map.entry(SET, "true")
        ), targetConfigOps);
        assertEquals("true", targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    private static void assertRemoteStorageDisabled(
        final Map<String, Entry<OpType, String>> targetConfigOps,
        final Map<String, String> targetConfigs
    ) {
        assertFalse(targetConfigOps.containsKey(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
        assertNull(targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }
}
