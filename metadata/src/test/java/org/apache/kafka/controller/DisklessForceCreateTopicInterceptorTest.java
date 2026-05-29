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

public class DisklessForceCreateTopicInterceptorTest {

    @Test
    public void forcesDisklessWhenTopicMatchesRegex() {
        final var interceptor = new DisklessForceCreateTopicInterceptor(List.of("my-topic-.*", "other-.*"));
        final Map<String, String> requestConfigs = new HashMap<>();
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptor.intercept("my-topic-1", requestConfigs, targetConfigOps);
        interceptor.intercept("my-topic-1", targetConfigs);

        assertEquals(Map.entry(SET, "true"), targetConfigOps.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertEquals("true", targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void forcesDisklessEvenWhenRequestHadDisklessFalse() {
        final var interceptor = new DisklessForceCreateTopicInterceptor(List.of("my-topic-.*"));
        final Map<String, String> requestConfigs = new HashMap<>(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));

        interceptor.intercept("my-topic-1", requestConfigs, targetConfigOps);
        interceptor.intercept("my-topic-1", targetConfigs);

        assertEquals(Map.entry(SET, "true"), targetConfigOps.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertEquals("true", targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void doesNotForceWhenTopicDoesNotMatchRegex() {
        final var interceptor = new DisklessForceCreateTopicInterceptor(List.of("my-topic-.*"));
        final Map<String, String> requestConfigs = new HashMap<>();
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptor.intercept("unmatched-topic", requestConfigs, targetConfigOps);
        interceptor.intercept("unmatched-topic", targetConfigs);

        assertFalse(targetConfigOps.containsKey(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void doesNotForceForSystemTopics() {
        final var interceptor = new DisklessForceCreateTopicInterceptor(List.of(".*"));
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

            assertFalse(targetConfigOps.containsKey(TopicConfig.DISKLESS_ENABLE_CONFIG),
                "Should not force diskless for system topic: " + topicName);
            assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG),
                "Should not force diskless for system topic: " + topicName);
        }
    }

    @Test
    public void doesNotForceForCompactedTopic() {
        final var interceptor = new DisklessForceCreateTopicInterceptor(List.of("my-topic-.*"));
        final Map<String, String> requestConfigs = Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
        );
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>(requestConfigs);

        interceptor.intercept("my-topic-compacted", requestConfigs, targetConfigOps);
        interceptor.intercept("my-topic-compacted", targetConfigs);

        assertFalse(targetConfigOps.containsKey(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void doesNotForceForCompactDeleteTopic() {
        final var interceptor = new DisklessForceCreateTopicInterceptor(List.of("my-topic-.*"));
        final Map<String, String> requestConfigs = Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE + "," + TopicConfig.CLEANUP_POLICY_COMPACT
        );
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>(requestConfigs);

        interceptor.intercept("my-topic-compact-delete", requestConfigs, targetConfigOps);
        interceptor.intercept("my-topic-compact-delete", targetConfigs);

        assertFalse(targetConfigOps.containsKey(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void matchesSecondRegexInList() {
        final var interceptor = new DisklessForceCreateTopicInterceptor(List.of("first-.*", "second-.*"));
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptor.intercept("second-topic", targetConfigs);

        assertEquals("true", targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void doesNotForceForTopicWithDoubleUnderscorePrefix() {
        final DisklessForceCreateTopicInterceptor interceptor =
            new DisklessForceCreateTopicInterceptor(List.of(".*"));

        final Map<String, String> targetConfigs = new HashMap<>();
        boolean result = interceptor.intercept("__consumer_offsets", targetConfigs);

        assertFalse(result);
        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void doesNotForceForTopicWithDoubleUnderscorePrefixOpType() {
        final DisklessForceCreateTopicInterceptor interceptor =
            new DisklessForceCreateTopicInterceptor(List.of(".*"));

        final Map<String, Map.Entry<OpType, String>> targetConfigOps = new HashMap<>();
        boolean result = interceptor.intercept("__custom_internal", Map.of(), targetConfigOps);

        assertFalse(result);
        assertNull(targetConfigOps.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }
}
