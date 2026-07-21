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

import java.util.Map;
import java.util.Map.Entry;

/**
 * Interface for intercepting CREATE_TOPICS API requests and mutating the topic configs.
 *
 * <p>Interceptors are chained and executed in order. Only the first interceptor that matches
 * (returns {@code true}) is applied; subsequent interceptors in the chain are skipped.
 *
 * <p>Each interceptor is controlled by a Controller config that enables or disables it.
 */
public interface CreateTopicConfigInterceptor {

    /**
     * Intercept topic creation and potentially mutate the target config operations.
     * This variant is used when building config records for persistence.
     *
     * @param topicName the name of the topic being created
     * @param requestConfigs the configs from the create topic request
     * @param targetConfigOps the mutable map of config operations to apply; interceptors may add entries
     * @return {@code true} if this interceptor matched and applied mutations, {@code false} otherwise
     */
    boolean intercept(
        String topicName,
        Map<String, String> requestConfigs,
        Map<String, Entry<OpType, String>> targetConfigOps
    );

    /**
     * Intercept topic creation and potentially mutate the target configs map directly.
     * This variant is used for validation and response payload generation.
     *
     * @param topicName the name of the topic being created
     * @param targetConfigs the mutable map of topic configs; interceptors may add or modify entries
     * @return {@code true} if this interceptor matched and applied mutations, {@code false} otherwise
     */
    boolean intercept(
        String topicName,
        Map<String, String> targetConfigs
    );
}
