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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CreateTopicConfigInterceptorsTest {

    @Test
    public void emptyInterceptorsDoNotModifyConfigs() {
        final Map<String, String> requestConfigs = new HashMap<>();
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        CreateTopicConfigInterceptors.EMPTY.intercept("test-topic", requestConfigs, targetConfigOps);
        CreateTopicConfigInterceptors.EMPTY.intercept("test-topic", targetConfigs);

        assertTrue(targetConfigOps.isEmpty());
        assertTrue(targetConfigs.isEmpty());
    }

    @Test
    public void noInterceptorWhenDisabled() {
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, false, false, List.of());
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("test-topic", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    @Test
    public void remoteStorageInterceptorAppliesWhenEnabled() {
        final var interceptors = CreateTopicConfigInterceptors.create(true, List.of(), false, false, false, List.of());
        final Map<String, String> requestConfigs = new HashMap<>();
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("test-topic", requestConfigs, targetConfigOps);
        interceptors.intercept("test-topic", targetConfigs);

        assertEquals(Map.entry(SET, "true"), targetConfigOps.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
        assertEquals("true", targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    @Test
    public void remoteStorageInterceptorDoesNotApplyForExcludedTopic() {
        final var interceptors = CreateTopicConfigInterceptors.create(true, List.of("excluded-.*"), false, false, false, List.of());
        final Map<String, String> requestConfigs = new HashMap<>();
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("excluded-topic", requestConfigs, targetConfigOps);
        interceptors.intercept("excluded-topic", targetConfigs);

        assertFalse(targetConfigOps.containsKey(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
        assertNull(targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    @Test
    public void disklessInterceptorAppliesWhenEnabledAndTopicMatches() {
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, true, true, List.of("my-topic-.*"));
        final Map<String, String> requestConfigs = new HashMap<>();
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("my-topic-1", requestConfigs, targetConfigOps);
        interceptors.intercept("my-topic-1", targetConfigs);

        assertEquals(Map.entry(SET, "true"), targetConfigOps.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertEquals("true", targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void disklessInterceptorDoesNotApplyWhenTopicDoesNotMatch() {
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, true, true, List.of("my-topic-.*"));
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("other-topic", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void disklessInterceptorTakesPriorityOverRemoteStorage() {
        // Both enabled, no explicit exclude regex needed — diskless wins by chain order
        final var interceptors = CreateTopicConfigInterceptors.create(
            true, List.of(), false,
            true, true, List.of("diskless-.*")
        );

        // A classic topic (not matching diskless regex) gets remote storage forced
        final Map<String, String> classicConfigs = new HashMap<>();
        interceptors.intercept("classic-topic", classicConfigs);
        assertEquals("true", classicConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
        assertNull(classicConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));

        // A diskless topic (matching diskless regex) gets diskless forced, NOT remote storage
        final Map<String, String> disklessConfigs = new HashMap<>();
        interceptors.intercept("diskless-topic", disklessConfigs);
        assertNull(disklessConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
        assertEquals("true", disklessConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void disklessInterceptorSkipsTopicsWithDoubleUnderscorePrefix() {
        final CreateTopicConfigInterceptors interceptors =
            CreateTopicConfigInterceptors.create(false, List.of(), false, true, true, List.of(".*"));

        final Map<String, String> targetConfigs = new HashMap<>();
        interceptors.intercept("__consumer_offsets", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void disklessInterceptorNotActivatedWhenSystemDisklessDisabled() {
        // disklessForceEnabled=true but disklessStorageSystemEnabled=false
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, false, true, List.of("my-topic-.*"));
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("my-topic-1", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void disklessDisabledFallbackConvertsTieredWhenExplicitlyFalse() {
        // disklessStorageSystemEnabled=false, no other interceptors
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, false, false, List.of());
        final Map<String, String> targetConfigs = new HashMap<>();
        targetConfigs.put(TopicConfig.DISKLESS_ENABLE_CONFIG, "false");

        interceptors.intercept("my-topic", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertEquals("true", targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    @Test
    public void disklessDisabledFallbackConvertsTieredViaConfigOps() {
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, false, false, List.of());
        final Map<String, String> requestConfigs = new HashMap<>();
        requestConfigs.put(TopicConfig.DISKLESS_ENABLE_CONFIG, "false");
        final Map<String, Entry<OpType, String>> targetConfigOps = new HashMap<>();
        targetConfigOps.put(TopicConfig.DISKLESS_ENABLE_CONFIG, Map.entry(SET, "false"));

        interceptors.intercept("my-topic", requestConfigs, targetConfigOps);

        assertFalse(targetConfigOps.containsKey(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertEquals(Map.entry(SET, "true"), targetConfigOps.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    @Test
    public void disklessDisabledFallbackDoesNotApplyWithoutExplicitConfig() {
        // No diskless.enable in configs — fallback should not match
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, false, false, List.of());
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("my-topic", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    @Test
    public void disklessDisabledFallbackDoesNotApplyForSystemTopics() {
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, false, false, List.of());
        final Map<String, String> targetConfigs = new HashMap<>();
        targetConfigs.put(TopicConfig.DISKLESS_ENABLE_CONFIG, "false");

        interceptors.intercept("__consumer_offsets", targetConfigs);

        // System topic should not be converted — diskless.enable stays
        assertEquals("false", targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertNull(targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    @Test
    public void disklessDisabledFallbackNotActiveWhenSystemEnabled() {
        // disklessStorageSystemEnabled=true — fallback should NOT be in the chain
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, true, false, List.of());
        final Map<String, String> targetConfigs = new HashMap<>();
        targetConfigs.put(TopicConfig.DISKLESS_ENABLE_CONFIG, "false");

        interceptors.intercept("my-topic", targetConfigs);

        // diskless.enable=false should remain untouched (no conversion to tiered)
        assertEquals("false", targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
        assertNull(targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }
}
