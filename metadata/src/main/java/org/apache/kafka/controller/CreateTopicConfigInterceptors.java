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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Container for CREATE_TOPICS config interceptors. Interceptors are chained and executed in order;
 * only the first interceptor that matches a topic is applied (first-match-wins).
 *
 * <p>The chain order is: diskless force interceptor first, then remote storage force interceptor.
 * This ensures that topics matching the diskless allow list are not also forced to remote storage.
 */
final class CreateTopicConfigInterceptors {
    static final CreateTopicConfigInterceptors EMPTY = new CreateTopicConfigInterceptors(List.of());

    private final List<CreateTopicConfigInterceptor> interceptors;

    private CreateTopicConfigInterceptors(final List<CreateTopicConfigInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * Creates the interceptors chain based on the provided configuration.
     * The diskless force interceptor is placed first in the chain so that matching topics
     * are handled by it before the remote storage force interceptor is considered.
     *
     * @param classicRemoteStorageForceEnabled whether the remote storage force interceptor is enabled
     * @param classicRemoteStorageForceExcludeTopicRegexes topic regexes to exclude from remote storage forcing
     * @param defaultDisklessEnable whether diskless is enabled by default
     * @param disklessForceEnabled whether the diskless force interceptor is enabled
     * @param disklessForceIncludeTopicRegexes topic regexes that define the allow list for diskless forcing
     * @return the configured interceptors chain
     */
    static CreateTopicConfigInterceptors create(
        final boolean classicRemoteStorageForceEnabled,
        final List<String> classicRemoteStorageForceExcludeTopicRegexes,
        final boolean defaultDisklessEnable,
        final boolean disklessForceEnabled,
        final List<String> disklessForceIncludeTopicRegexes
    ) {
        final List<CreateTopicConfigInterceptor> chain = new ArrayList<>();
        // Diskless interceptor is first in the chain (higher priority)
        if (disklessForceEnabled) {
            chain.add(new DisklessForceCreateTopicInterceptor(disklessForceIncludeTopicRegexes));
        }
        if (classicRemoteStorageForceEnabled) {
            chain.add(new ClassicTopicRemoteStorageForceCreateTopicInterceptor(classicRemoteStorageForceExcludeTopicRegexes, defaultDisklessEnable));
        }
        if (chain.isEmpty()) {
            return EMPTY;
        }
        return new CreateTopicConfigInterceptors(List.copyOf(chain));
    }

    /**
     * Apply the interceptor chain. The first interceptor that matches the topic is applied;
     * subsequent interceptors are skipped.
     *
     * @param topicName the name of the topic being created
     * @param requestConfigs the configs from the create topic request
     * @param targetConfigOps the mutable map of config operations to apply
     */
    void intercept(
        final String topicName,
        final Map<String, String> requestConfigs,
        final Map<String, Entry<OpType, String>> targetConfigOps
    ) {
        for (CreateTopicConfigInterceptor interceptor : interceptors) {
            if (interceptor.intercept(topicName, requestConfigs, targetConfigOps)) {
                return;
            }
        }
    }

    /**
     * Apply the interceptor chain. The first interceptor that matches the topic is applied;
     * subsequent interceptors are skipped.
     *
     * @param topicName the name of the topic being created
     * @param targetConfigs the mutable map of topic configs
     */
    void intercept(
        final String topicName,
        final Map<String, String> targetConfigs
    ) {
        for (CreateTopicConfigInterceptor interceptor : interceptors) {
            if (interceptor.intercept(topicName, targetConfigs)) {
                return;
            }
        }
    }
}
