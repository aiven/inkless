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
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;

/**
 * A {@link CreateTopicConfigInterceptor} that converts topics with explicit {@code diskless.enable=false}
 * into tiered topics ({@code remote.storage.enable=true}) when the diskless storage system is globally
 * disabled.
 *
 * <p>This prevents topic creation from failing with "It is invalid to set diskless.enable if diskless
 * storage system is not enabled" when a client explicitly opts out of diskless. Instead, the topic is
 * transparently created as a tiered topic.
 *
 * <p>Only activates when {@code diskless.storage.system.enable=false}. Excluded: system topics.
 */
final class DisklessDisabledFallbackCreateTopicInterceptor implements CreateTopicConfigInterceptor {

    @Override
    public boolean intercept(
        final String topicName,
        final Map<String, String> requestConfigs,
        final Map<String, Entry<OpType, String>> targetConfigOps
    ) {
        if (shouldConvertToTiered(topicName, requestConfigs)) {
            targetConfigOps.remove(TopicConfig.DISKLESS_ENABLE_CONFIG);
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
        if (shouldConvertToTiered(topicName, targetConfigs)) {
            targetConfigs.remove(TopicConfig.DISKLESS_ENABLE_CONFIG);
            targetConfigs.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
            return true;
        }
        return false;
    }

    private boolean shouldConvertToTiered(final String topicName, final Map<String, String> configs) {
        if (ReplicationControlManager.isSystemTopic(topicName)) {
            return false;
        }
        String disklessValue = configs.get(TopicConfig.DISKLESS_ENABLE_CONFIG);
        return disklessValue != null && "false".equalsIgnoreCase(disklessValue.trim());
    }
}
