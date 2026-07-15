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
package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Shared helpers for reading and polling topic configuration via {@link Admin} in integration tests.
 *
 * <p>After {@code createTopics} returns, the controller has committed the topic, but the
 * metadata propagates to brokers asynchronously — a subsequent {@code describeConfigs} call may
 * throw {@link UnknownTopicOrPartitionException} (topic not yet visible) or return stale config
 * values. After {@code incrementalAlterConfigs} returns, the topic is already visible but the
 * broker's metadata cache may still serve the old config value. The helpers here retry on
 * {@link UnknownTopicOrPartitionException} and (for {@link #waitForTopicConfigValue}) poll until
 * the value converges.
 */
public final class TopicConfigTestUtils {

    private TopicConfigTestUtils() {
    }

    /**
     * Read all config entries for a topic via {@code describeConfigs}. Retries up to 3 times
     * (1s delay) on {@link UnknownTopicOrPartitionException} to ride out metadata propagation
     * delays after topic creation.
     */
    public static Map<String, String> getTopicConfig(Admin admin, String topic)
            throws ExecutionException, InterruptedException, TimeoutException {
        final int maxRetries = 3;
        final long retryDelayMs = 1000;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                final var config = admin.describeConfigs(Collections.singletonList(resource))
                    .all().get(10, TimeUnit.SECONDS)
                    .get(resource);
                if (config == null) {
                    throw new IllegalStateException("describeConfigs returned no config for topic " + topic);
                }
                final Map<String, String> configs = new HashMap<>();
                config.entries().forEach(entry -> configs.put(entry.name(), entry.value()));
                return configs;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException && attempt < maxRetries) {
                    Thread.sleep(retryDelayMs);
                } else {
                    throw e;
                }
            }
        }
        throw new IllegalStateException("Exited retry loop unexpectedly.");
    }

    /**
     * Read a single topic config value via {@code describeConfigs}. Retries on transient
     * {@link UnknownTopicOrPartitionException} (see {@link #getTopicConfig}).
     *
     * @throws IllegalStateException if the config {@code key} is absent for the topic (unlike
     *         {@code getTopicConfig(...).get(key)}, which returns {@code null} in that case)
     */
    public static String getTopicConfigValue(Admin admin, String topic, String key)
            throws ExecutionException, InterruptedException, TimeoutException {
        final String value = getTopicConfig(admin, topic).get(key);
        if (value == null) {
            throw new IllegalStateException("Config '" + key + "' is missing for topic " + topic);
        }
        return value;
    }

    /**
     * Poll until the given topic config {@code key} reaches {@code expectedValue}.
     *
     * <p>After {@code createTopics} returns, the topic may not yet be visible to the broker
     * serving the request ({@link UnknownTopicOrPartitionException}); after
     * {@code incrementalAlterConfigs} returns, the topic is visible but the broker may still
     * serve the old config value. This method polls (60s deadline) until the value converges,
     * treating {@link UnknownTopicOrPartitionException} and {@link TimeoutException} as transient.
     */
    public static void waitForTopicConfigValue(Admin admin, String topic, String key, String expectedValue)
            throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                return expectedValue.equals(getTopicConfigValue(admin, topic, key));
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                }
                throw new NoRetryException(e.getCause() != null ? e.getCause() : e);
            } catch (TimeoutException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NoRetryException(e);
            }
        }, 60_000, () -> "Topic config " + key + " for topic " + topic + " should become " + expectedValue);
    }

    /**
     * Wait until the topic is visible in the broker metadata. {@code createTopics} returns once
     * the controller has committed the topic, but the metadata propagates to brokers asynchronously.
     * Until propagation completes, {@code describeTopicPartitions} fails with
     * {@link UnknownTopicOrPartitionException}. This method polls (30s deadline) until the
     * topic is visible, treating {@link UnknownTopicOrPartitionException} and
     * {@link TimeoutException} as transient.
     */
    public static void waitForTopicToExist(Admin admin, String topic) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                final DescribeTopicPartitionsResponseData responseData =
                    admin.describeTopicPartitions(List.of(topic)).rawResponse().get(10, TimeUnit.SECONDS);
                return responseData.topics().find(topic) != null;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                }
                throw new NoRetryException(e.getCause() != null ? e.getCause() : e);
            } catch (TimeoutException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NoRetryException(e);
            }
        }, 30_000, () -> "Topic " + topic + " was not visible in broker metadata within 30s");
    }
}
