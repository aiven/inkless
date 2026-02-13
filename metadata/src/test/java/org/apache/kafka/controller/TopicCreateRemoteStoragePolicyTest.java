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

import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TopicCreateRemoteStoragePolicyTest {
    @Test
    void policyDisabledKeepsBehaviorUnchanged() {
        TopicCreateRemoteStoragePolicy policy = new TopicCreateRemoteStoragePolicy(false);
        assertNull(policy.apply("foo", null, false));
    }

    @Test
    void classicTopicDefaultsToRemoteStorageEnabled() {
        TopicCreateRemoteStoragePolicy policy = new TopicCreateRemoteStoragePolicy(true);
        Map<String, Map.Entry<OpType, String>> result = policy.apply("foo", null, false);
        assertEquals("true", result.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());
    }

    @Test
    void classicTopicOverridesExplicitRemoteStorageFalse() {
        TopicCreateRemoteStoragePolicy policy = new TopicCreateRemoteStoragePolicy(true);
        Map<String, Map.Entry<OpType, String>> keyToOps = new HashMap<>();
        keyToOps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, new SimpleImmutableEntry<>(SET, "false"));
        Map<String, Map.Entry<OpType, String>> result = policy.apply("foo", keyToOps, false);
        assertEquals("true", result.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());
    }

    @Test
    void disklessForcesRemoteStorageDisabled() {
        TopicCreateRemoteStoragePolicy policy = new TopicCreateRemoteStoragePolicy(true);
        Map<String, Map.Entry<OpType, String>> result = policy.apply("foo", null, true);
        assertEquals("false", result.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());
    }

    @Test
    void internalTopicsForceRemoteStorageDisabled() {
        TopicCreateRemoteStoragePolicy policy = new TopicCreateRemoteStoragePolicy(true);
        assertEquals("false", policy.apply("__consumer_offsets", null, false)
            .get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());
        assertEquals("false", policy.apply("__any_internal_topic", null, false)
            .get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());
        assertEquals("false", policy.apply("__connect_configs-test", null, false)
            .get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());
    }

    @Test
    void excludeSpecificTopics() {
        TopicCreateRemoteStoragePolicy policy = new TopicCreateRemoteStoragePolicy(
            true,
            "_schemas|mm2-(.*)"
        );
        assertEquals("false", policy.apply("_schemas", null, false)
            .get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());

        assertEquals("false", policy.apply("mm2-test", null, false)
            .get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());

        assertEquals("true", policy.apply("mytopic", null, false)
            .get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());
    }

    @Test
    void invalidCustomRegexFailsFast() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new TopicCreateRemoteStoragePolicy(true, "my-custom-(")
        );
    }
}
