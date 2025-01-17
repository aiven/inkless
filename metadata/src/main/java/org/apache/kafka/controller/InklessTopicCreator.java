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
// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.metadata.placement.UsableBroker;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.TOPIC_AUTHORIZATION_FAILED;
import static org.apache.kafka.controller.ReplicationControlManager.translateCreationConfigs;

/**
 * Topic creation handler for Inkless.
 */
public class InklessTopicCreator {
    final ConfigurationControlManager configurationControl;
    final ClusterControlManager clusterControl;
    final FeatureControlManager featureControl;
    final int defaultNumPartitions;
    final Logger log;

    public InklessTopicCreator(final ConfigurationControlManager configurationControl,
                               final ClusterControlManager clusterControl,
                               final FeatureControlManager featureControl,
                               final int defaultNumPartitions,
                               final Logger log) {
        this.configurationControl = configurationControl;
        this.clusterControl = clusterControl;
        this.featureControl = featureControl;
        this.defaultNumPartitions = defaultNumPartitions;
        this.log = log;
    }

    ApiError createTopic(final ControllerRequestContext context,
                         final CreateTopicsRequestData.CreatableTopic topic,
                         final List<ApiMessageAndVersion> records,
                         final Map<String, CreateTopicsResponseData.CreatableTopicResult> successes,
                         final List<ApiMessageAndVersion> configRecords,
                         final boolean authorizedToReturnConfigs,
                         final Function<Supplier<CreateTopicPolicy.RequestMetadata>, ApiError> maybeCheckCreateTopicPolicy) {
        final Map<String, String> creationConfigs = translateCreationConfigs(topic.configs());

        // Specific validation for inkless topics
        final ApiError validationError = validateTopic(topic);
        if (validationError != null) return validationError;

        final short replicationFactor = 1;
        final int numPartitions = topic.numPartitions() == -1 ? defaultNumPartitions : topic.numPartitions();

        final Iterator<UsableBroker> usableBrokers = clusterControl.usableBrokers();
        if (!usableBrokers.hasNext()) {
            return new ApiError(Errors.BROKER_NOT_AVAILABLE, "No brokers available to create inkless topic.");
        }

        // Define a fixed leader for all partitions. This is just for response purposes.
        // Actual leader will be defined by Inkless Control Plane.
        final Map<Integer, PartitionRegistration> newParts = definePartitions(numPartitions, usableBrokers.next());

        // from here follow the same logic as ReplicationControlManager#createTopic
        try {
            context.applyPartitionChangeQuota(numPartitions); // check controller mutation quota
        } catch (ThrottlingQuotaExceededException e) {
            log.debug("Topic creation of {} partitions not allowed because quota is violated. Delay time: {}",
                numPartitions, e.throttleTimeMs());
            return ApiError.fromThrowable(e);
        }

        final ApiError error = maybeCheckCreateTopicPolicy.apply(() -> new CreateTopicPolicy.RequestMetadata(
            topic.name(), numPartitions, replicationFactor, null, creationConfigs));
        if (error.isFailure()) return error;

        final Uuid topicId = Uuid.randomUuid();
        final CreateTopicsResponseData.CreatableTopicResult result = new CreateTopicsResponseData.CreatableTopicResult().
            setName(topic.name()).
            setTopicId(topicId).
            setErrorCode(NONE.code()).
            setErrorMessage(null);
        if (authorizedToReturnConfigs) {
            final Map<String, ConfigEntry> effectiveConfig = configurationControl.computeEffectiveTopicConfigs(creationConfigs);
            final List<String> configNames = new ArrayList<>(effectiveConfig.keySet());
            configNames.sort(String::compareTo);
            for (String configName : configNames) {
                final ConfigEntry entry = effectiveConfig.get(configName);
                result.configs().add(new CreateTopicsResponseData.CreatableTopicConfigs().
                    setName(entry.name()).
                    setValue(entry.isSensitive() ? null : entry.value()).
                    setReadOnly(entry.isReadOnly()).
                    setConfigSource(KafkaConfigSchema.translateConfigSource(entry.source()).id()).
                    setIsSensitive(entry.isSensitive()));
            }
            result.setNumPartitions(numPartitions);
            result.setReplicationFactor(replicationFactor);
            result.setTopicConfigErrorCode(NONE.code());
        } else {
            result.setTopicConfigErrorCode(TOPIC_AUTHORIZATION_FAILED.code());
        }
        successes.put(topic.name(), result);
        records.add(new ApiMessageAndVersion(new TopicRecord().
            setName(topic.name()).
            setTopicId(topicId), (short) 0));
        // ConfigRecords go after TopicRecord but before PartitionRecord(s).
        records.addAll(configRecords);
        for (Map.Entry<Integer, PartitionRegistration> partEntry : newParts.entrySet()) {
            int partitionIndex = partEntry.getKey();
            PartitionRegistration info = partEntry.getValue();
            records.add(info.toRecord(topicId, partitionIndex, new ImageWriterOptions.Builder().
                setMetadataVersion(featureControl.metadataVersion()).
                build()));
        }
        return ApiError.NONE;
    }

    private static ApiError validateTopic(final CreateTopicsRequestData.CreatableTopic topic) {
        if (Math.abs(topic.replicationFactor()) != 1) {
            return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                "Replication factor for Inkless topics must be 1 or -1 to use the default value (1).");
        } else if (topic.numPartitions() < -1 || topic.numPartitions() == 0) {
            return new ApiError(Errors.INVALID_PARTITIONS,
                "Number of partitions was set to an invalid non-positive value.");
        }
        return null;
    }

    private Map<Integer, PartitionRegistration> definePartitions(final int numPartitions, final UsableBroker firstBroker) {
        final Map<Integer, PartitionRegistration> newParts = new HashMap<>();
        final int leader = firstBroker.id();
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            newParts.put(
                partitionId,
                new PartitionRegistration.Builder().
                    setReplicas(Replicas.toArray(List.of(leader))).
                    setDirectories(Uuid.toArray(List.of(clusterControl.defaultDir(leader)))).
                    setIsr(Replicas.toArray(List.of(leader))).
                    setLeader(leader).
                    setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                    setLeaderEpoch(0).
                    setPartitionEpoch(0).
                    build()
            );
        }
        return newParts;
    }
}
