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
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, false, List.of());
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("test-topic", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }

    @Test
    public void remoteStorageInterceptorAppliesWhenEnabled() {
        final var interceptors = CreateTopicConfigInterceptors.create(true, List.of(), false, false, List.of());
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
        final var interceptors = CreateTopicConfigInterceptors.create(true, List.of("excluded-.*"), false, false, List.of());
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
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, true, List.of("my-topic-.*"));
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
        final var interceptors = CreateTopicConfigInterceptors.create(false, List.of(), false, true, List.of("my-topic-.*"));
        final Map<String, String> targetConfigs = new HashMap<>();

        interceptors.intercept("other-topic", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    @Test
    public void disklessInterceptorTakesPriorityOverRemoteStorage() {
        // Both enabled, no explicit exclude regex needed — diskless wins by chain order
        final var interceptors = CreateTopicConfigInterceptors.create(
            true, List.of(), false,
            true, List.of("diskless-.*")
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
            CreateTopicConfigInterceptors.create(false, List.of(), false, true, List.of(".*"));

        final Map<String, String> targetConfigs = new HashMap<>();
        interceptors.intercept("__consumer_offsets", targetConfigs);

        assertNull(targetConfigs.get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }
}
