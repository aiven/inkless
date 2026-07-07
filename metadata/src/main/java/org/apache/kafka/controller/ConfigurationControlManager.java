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

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.message.CreateClusterMirrorResponseData;
import org.apache.kafka.common.message.DeleteClusterMirrorResponseData;
import org.apache.kafka.common.message.PauseMirrorTopicsResponseData;
import org.apache.kafka.common.message.ResumeMirrorTopicsResponseData;
import org.apache.kafka.common.message.StartMirrorTopicsResponseData;
import org.apache.kafka.common.message.StopMirrorTopicsResponseData;
import org.apache.kafka.common.metadata.ClearElrRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.MirrorTopicStateChangeRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.server.mutable.BoundedList;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.apache.kafka.server.util.MirrorFilterUtils;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import org.slf4j.Logger;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.DELETE;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;
import static org.apache.kafka.common.protocol.Errors.CLUSTER_MIRROR_ALREADY_EXISTS;
import static org.apache.kafka.common.protocol.Errors.INVALID_CONFIG;
import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;

public class ConfigurationControlManager {
    public static final ConfigResource DEFAULT_NODE = new ConfigResource(Type.BROKER, "");

    private final Logger log;
    private final SnapshotRegistry snapshotRegistry;
    private final KafkaConfigSchema configSchema;
    private final Consumer<ConfigResource> existenceChecker;
    private final Optional<AlterConfigPolicy> alterConfigPolicy;
    private final ConfigurationValidator validator;
    private final TimelineHashMap<ConfigResource, TimelineHashMap<String, String>> configData;
    private final TimelineHashSet<Integer> brokersWithConfigs;
    private final Map<String, Object> staticConfig;
    private final ConfigResource currentController;
    private final FeatureControlManager featureControl;

    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private KafkaConfigSchema configSchema = null;
        private Consumer<ConfigResource> existenceChecker = __ -> { };
        private Optional<AlterConfigPolicy> alterConfigPolicy = Optional.empty();
        private ConfigurationValidator validator = ConfigurationValidator.NO_OP;
        private Map<String, Object> staticConfig = Map.of();
        private int nodeId = 0;
        private FeatureControlManager featureControl = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setKafkaConfigSchema(KafkaConfigSchema configSchema) {
            this.configSchema = configSchema;
            return this;
        }

        Builder setExistenceChecker(Consumer<ConfigResource> existenceChecker) {
            this.existenceChecker = existenceChecker;
            return this;
        }

        Builder setAlterConfigPolicy(Optional<AlterConfigPolicy> alterConfigPolicy) {
            this.alterConfigPolicy = alterConfigPolicy;
            return this;
        }

        Builder setValidator(ConfigurationValidator validator) {
            this.validator = validator;
            return this;
        }

        Builder setStaticConfig(Map<String, Object> staticConfig) {
            this.staticConfig = staticConfig;
            return this;
        }

        Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        Builder setFeatureControl(FeatureControlManager featureControl) {
            this.featureControl = featureControl;
            return this;
        }

        ConfigurationControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (configSchema == null) {
                throw new RuntimeException("You must set the configSchema.");
            }
            if (featureControl == null) {
                featureControl = new FeatureControlManager.Builder().build();
            }
            return new ConfigurationControlManager(
                logContext,
                snapshotRegistry,
                configSchema,
                existenceChecker,
                alterConfigPolicy,
                validator,
                staticConfig,
                nodeId,
                featureControl);
        }
    }

    private ConfigurationControlManager(LogContext logContext,
            SnapshotRegistry snapshotRegistry,
            KafkaConfigSchema configSchema,
            Consumer<ConfigResource> existenceChecker,
            Optional<AlterConfigPolicy> alterConfigPolicy,
            ConfigurationValidator validator,
            Map<String, Object> staticConfig,
            int nodeId,
            FeatureControlManager featureControl
    ) {
        this.log = logContext.logger(ConfigurationControlManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.configSchema = configSchema;
        this.existenceChecker = existenceChecker;
        this.alterConfigPolicy = alterConfigPolicy;
        this.validator = validator;
        this.configData = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokersWithConfigs = new TimelineHashSet<>(snapshotRegistry, 0);
        this.staticConfig = Map.copyOf(staticConfig);
        this.currentController = new ConfigResource(Type.BROKER, Integer.toString(nodeId));
        this.featureControl = featureControl;
    }

    SnapshotRegistry snapshotRegistry() {
        return snapshotRegistry;
    }

    /**
     * Determine the result of applying a batch of incremental configuration changes.  Note
     * that this method does not change the contents of memory.  It just generates a
     * result, that you can replay later if you wish using replay().
     *
     * Note that there can only be one result per ConfigResource.  So if you try to modify
     * several keys and one modification fails, the whole ConfigKey fails and nothing gets
     * changed.
     *
     * @param configChanges     Maps each resource to a map from config keys to
     *                          operation data.
     * @return                  The result.
     */
    ControllerResult<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        Map<ConfigResource, ApiError> outputResults = new HashMap<>();
        for (Entry<ConfigResource, Map<String, Entry<OpType, String>>> resourceEntry :
                configChanges.entrySet()) {
            ApiError apiError = incrementalAlterConfigResource(resourceEntry.getKey(),
                resourceEntry.getValue(),
                newlyCreatedResource,
                outputRecords);
            outputResults.put(resourceEntry.getKey(), apiError);
        }
        outputRecords.addAll(createClearElrRecordsAsNeeded(outputRecords));
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    ControllerResult<CreateClusterMirrorResponseData> addMirrorConfig(
        String mirrorName,
        Map<String, Map.Entry<AlterConfigOp.OpType, String>> configChanges
    ) {
        List<ApiMessageAndVersion> outputRecords = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        CreateClusterMirrorResponseData data = new CreateClusterMirrorResponseData();

        ConfigResource configResource = new ConfigResource(Type.CLUSTER_MIRROR, mirrorName);
        final Map<String, String> existingConfig = getConfigs(configResource);
        if (!existingConfig.isEmpty()) {
            data.setErrorCode(CLUSTER_MIRROR_ALREADY_EXISTS.code())
                .setErrorMessage("Mirror '" + mirrorName + "' already exists");
            return ControllerResult.of(List.of(), data);
        }

        ApiError apiError = incrementalAlterConfigResource(configResource,
            configChanges,
            false,
            outputRecords);
        // TODO: Should handle the error here
        log.info("!!! addMirrorConfig apiError: {} for {} {}", apiError, configResource, configChanges);
        outputRecords.addAll(createClearElrRecordsAsNeeded(outputRecords));

        data.setErrorCode((short) 0);

        return ControllerResult.atomicOf(outputRecords, data);
    }

    ControllerResult<StartMirrorTopicsResponseData> startMirrorTopics(
            String mirrorName,
            List<Controller.MirrorTopicMetadata> topics,
            List<String> includePatterns,
            List<String> excludePatterns,
            ReplicationControlManager replicationControl) {
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        StartMirrorTopicsResponseData data = new StartMirrorTopicsResponseData();

        Set<String> topicNames = topics.stream().map(Controller.MirrorTopicMetadata::name).collect(Collectors.toSet());
        ApiError patternError = updatePatternsAndStopExcluded(mirrorName, records, topicNames, replicationControl, (includeSet, excludeSet) -> {
            for (String topicName : topicNames) {
                includeSet.add(topicName);
                excludeSet.remove(topicName);
            }
            for (String pattern : includePatterns) {
                includeSet.add(pattern);
                excludeSet.remove(pattern);
            }
            for (String pattern : excludePatterns) {
                excludeSet.add(pattern);
                includeSet.remove(pattern);
            }
        });
        if (patternError.isFailure()) {
            data.setErrorCode(patternError.error().code());
            return ControllerResult.of(records, data);
        }

        List<StartMirrorTopicsResponseData.TopicResult> topicResList = new ArrayList<>();
        for (Controller.MirrorTopicMetadata topic : topics) {
            StartMirrorTopicsResponseData.TopicResult topicRes = new StartMirrorTopicsResponseData.TopicResult();
            String topicName = topic.name();
            topicRes.setName(topicName);

            ReplicationControlManager.TopicControlInfo existingTopic = replicationControl.getTopicByName(topicName);
            ReplicationControlManager.TopicControlInfo existingIdTopic = replicationControl.getTopic(topic.id());

            boolean hasRequestTopicId = !topic.id().equals(Uuid.ZERO_UUID);
            boolean topicIdMismatch = existingTopic != null && hasRequestTopicId
                    && !existingTopic.topicId().equals(topic.id());
            boolean topicNameMismatch = existingIdTopic != null && hasRequestTopicId
                    && !existingIdTopic.name().equals(topicName);
            boolean activeInOtherMirror = existingTopic != null && existingTopic.mirrorName() != null
                    && !existingTopic.mirrorName().isBlank()
                    && !existingTopic.mirrorName().equals(mirrorName)
                    && existingTopic.mirrorState() != MirrorPartitionState.STOPPED.value();
            boolean startedState = existingTopic != null && existingTopic.mirrorName() != null
                    && existingTopic.mirrorName().equals(mirrorName)
                    && existingTopic.mirrorState() == MirrorPartitionState.MIRRORING.value();
            boolean nonStoppedState = existingTopic != null && existingTopic.mirrorName() != null
                    && existingTopic.mirrorName().equals(mirrorName)
                    && existingTopic.mirrorState() != MirrorPartitionState.STOPPED.value();

            if (topicIdMismatch) {
                topicRes.setErrorCode(Errors.INCONSISTENT_TOPIC_ID.code())
                        .setErrorMessage("Topic id " + topic.id() + " in the request doesn't match " +
                                "the existing topic id " + existingTopic.topicId() + " for topic " + topicName);
            }

            if (topicNameMismatch) {
                topicRes.setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                        .setErrorMessage("Topic id " + topic.id() + " with topic name " + topic.name() +
                                " in the request is already used by topic name " + existingIdTopic.name());
            }

            if (activeInOtherMirror) {
                topicRes.setErrorCode(Errors.TOPIC_ALREADY_IN_CLUSTER_MIRROR.code())
                        .setErrorMessage("Topic '" + topicName + "' is already in mirror '" + existingTopic.mirrorName()
                                + "' in " + MirrorPartitionState.fromValue((byte) existingTopic.mirrorState()) + " state");
                topicResList.add(topicRes);
                continue;
            }

            Uuid topicId = topic.id();
            boolean hasNoPartitionInfo = existingTopic == null && topic.numPartitions() <= 0;
            if (hasNoPartitionInfo) {
                log.warn("Topic {} for mirror {} has no topic ID or partition info and will be" +
                        " created at the next metadata refresh", topicName, mirrorName);
                topicResList.add(topicRes);
                continue;
            }
            if (topicId.equals(Uuid.ZERO_UUID)) {
                topicId = existingTopic != null ? existingTopic.topicId() : Uuid.randomUuid();
            }

            if (topic.numPartitions() > 0) {
                ApiError createError = replicationControl.createMirrorTopic(
                        topicName, topicId, topic.numPartitions(), records);
                if (createError.isFailure() && createError.error() != Errors.TOPIC_ALREADY_EXISTS) {
                    topicRes.setErrorCode(createError.error().code());
                    topicResList.add(topicRes);
                    continue;
                }
            }

            if (startedState) {
                topicResList.add(topicRes);
                continue;
            } else if (nonStoppedState) {
                topicRes.setErrorCode(Errors.MIRROR_TOPIC_NOT_STOPPED.code())
                        .setErrorMessage("Topic '" + topicName + "' is in "
                                + MirrorPartitionState.fromValue((byte) existingTopic.mirrorState()) + " state");
                topicResList.add(topicRes);
                continue;
            }

            records.add(new ApiMessageAndVersion(
                    new MirrorTopicStateChangeRecord()
                            .setTopicId(topicId)
                            .setMirrorName(mirrorName)
                            .setDesiredState(MirrorPartitionState.MIRRORING.value()),
                    (short) 0));

            topicResList.add(topicRes);
        }
        data.setTopics(topicResList);

        for (StartMirrorTopicsResponseData.TopicResult tr : topicResList) {
            if (tr.errorCode() != Errors.NONE.code()) {
                data.setErrorCode(tr.errorCode());
                data.setErrorMessage(tr.errorMessage());
                break;
            }
        }

        return ControllerResult.of(records, data);
    }

    ControllerResult<StopMirrorTopicsResponseData> stopMirrorTopics(String mirrorName, Set<String> topics, List<String> patterns, ReplicationControlManager replicationControl) {
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        StopMirrorTopicsResponseData data = new StopMirrorTopicsResponseData();

        if (!patterns.isEmpty()) {
            ApiError patternError = updatePatternsAndStopExcluded(mirrorName, records, Set.of(), replicationControl, (includeSet, excludeSet) -> {
                for (String pattern : patterns) {
                    if (!includeSet.remove(pattern)) {
                        excludeSet.add(pattern);
                    }
                }
            });
            if (patternError.isFailure()) {
                data.setErrorCode(patternError.error().code());
                return ControllerResult.of(records, data);
            }
        }

        List<StopMirrorTopicsResponseData.TopicResult> topicResList = new ArrayList<>();
        for (String topic : topics) {
            StopMirrorTopicsResponseData.TopicResult topicRes = new StopMirrorTopicsResponseData.TopicResult();
            Uuid topicId = replicationControl.getTopicId(topic);
            ReplicationControlManager.TopicControlInfo topicInfo = replicationControl.getTopic(topicId);
            if (topicInfo == null) {
                topicRes.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            String currMirrorName = topicInfo.mirrorName();
            int currMirrorStateChange = topicInfo.mirrorState();
            if (currMirrorName == null || currMirrorName.isBlank()) {
                topicRes.setErrorCode(Errors.UNKNOWN_CLUSTER_MIRROR.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            if (!currMirrorName.equals(mirrorName)) {
                topicRes.setErrorCode(Errors.TOPIC_NOT_IN_CLUSTER_MIRROR.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            if (currMirrorStateChange == MirrorPartitionState.STOPPED.value()) {
                topicRes.setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            if (currMirrorStateChange == MirrorPartitionState.PAUSED.value()) {
                topicRes.setName(topic).setErrorCode(Errors.MIRROR_TOPIC_ALREADY_PAUSED.code());
                topicResList.add(topicRes);
                continue;
            }

            records.add(new ApiMessageAndVersion(
                    new MirrorTopicStateChangeRecord()
                            .setTopicId(topicId)
                            .setMirrorName(mirrorName)
                            .setDesiredState(MirrorPartitionState.STOPPED.value()),
                    (short) 0));

            topicRes.setName(topic);
            topicResList.add(topicRes);
        }
        data.setTopics(topicResList);

        return ControllerResult.of(records, data);
    }

    ControllerResult<PauseMirrorTopicsResponseData> pauseMirrorTopics(String mirrorName, Set<String> topics, ReplicationControlManager replicationControl) {
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        PauseMirrorTopicsResponseData data = new PauseMirrorTopicsResponseData();
        List<PauseMirrorTopicsResponseData.TopicResult> topicResList = new ArrayList<>();
        for (String topic : topics) {
            PauseMirrorTopicsResponseData.TopicResult topicRes = new PauseMirrorTopicsResponseData.TopicResult();
            Uuid topicId = replicationControl.getTopicId(topic);
            ReplicationControlManager.TopicControlInfo topicInfo = replicationControl.getTopic(topicId);
            if (topicInfo == null) {
                topicRes.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            String currMirrorName = topicInfo.mirrorName();
            int currMirrorStateChange = topicInfo.mirrorState();
            if (currMirrorName == null || currMirrorName.isBlank()) {
                topicRes.setErrorCode(Errors.UNKNOWN_CLUSTER_MIRROR.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            if (!currMirrorName.equals(mirrorName)) {
                topicRes.setErrorCode(Errors.TOPIC_NOT_IN_CLUSTER_MIRROR.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            if (currMirrorStateChange == MirrorPartitionState.STOPPED.value()) {
                topicRes.setErrorCode(Errors.MIRROR_TOPIC_BEING_STOPPED.code()).setName(topic)
                        .setErrorMessage("Topic '" + topic + "' is in "
                                + MirrorPartitionState.fromValue((byte) currMirrorStateChange) + " state");
                topicResList.add(topicRes);
                continue;
            }

            if (currMirrorStateChange == MirrorPartitionState.PAUSED.value()) {
                topicRes.setErrorCode(Errors.NONE.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            records.add(new ApiMessageAndVersion(
                new MirrorTopicStateChangeRecord()
                    .setTopicId(topicId)
                    .setMirrorName(mirrorName)
                    .setDesiredState(MirrorPartitionState.PAUSED.value()),
                (short) 0));
        }
        data.setTopics(topicResList);

        return ControllerResult.of(records, data);
    }

    ControllerResult<ResumeMirrorTopicsResponseData> resumeMirrorTopics(String mirrorName, Set<String> topics, ReplicationControlManager replicationControl) {
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        ResumeMirrorTopicsResponseData data = new ResumeMirrorTopicsResponseData();
        List<ResumeMirrorTopicsResponseData.TopicResult> topicResList = new ArrayList<>();
        for (String topic : topics) {
            ResumeMirrorTopicsResponseData.TopicResult topicRes = new ResumeMirrorTopicsResponseData.TopicResult();
            Uuid topicId = replicationControl.getTopicId(topic);
            ReplicationControlManager.TopicControlInfo topicInfo = replicationControl.getTopic(topicId);
            if (topicInfo == null) {
                topicRes.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            String currMirrorName = topicInfo.mirrorName();
            int currMirrorStateChange = topicInfo.mirrorState();
            if (currMirrorName == null || currMirrorName.isBlank()) {
                topicRes.setErrorCode(Errors.UNKNOWN_CLUSTER_MIRROR.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            if (!currMirrorName.equals(mirrorName)) {
                topicRes.setErrorCode(Errors.TOPIC_NOT_IN_CLUSTER_MIRROR.code()).setName(topic);
                topicResList.add(topicRes);
                continue;
            }

            if (currMirrorStateChange == MirrorPartitionState.MIRRORING.value()) {
                // it's already in mirroring state, skip it
                topicResList.add(topicRes);
                continue;
            } else if (currMirrorStateChange != MirrorPartitionState.PAUSED.value()) {
                topicRes.setErrorCode(Errors.MIRROR_TOPIC_NOT_PAUSED.code()).setName(topic)
                        .setErrorMessage("Topic '" + topic + "' is in "
                                + MirrorPartitionState.fromValue((byte) currMirrorStateChange) + " state");
                topicResList.add(topicRes);
                continue;
            }

            records.add(new ApiMessageAndVersion(
                new MirrorTopicStateChangeRecord()
                    .setTopicId(topicId)
                    .setMirrorName(mirrorName)
                    .setDesiredState(MirrorPartitionState.MIRRORING.value()),
                (short) 0));
        }
        data.setTopics(topicResList);

        return ControllerResult.of(records, data);
    }

    ControllerResult<DeleteClusterMirrorResponseData> deleteClusterMirror(String mirrorName, long stateValidationOffset, ReplicationControlManager replicationControl) {
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        DeleteClusterMirrorResponseData data = new DeleteClusterMirrorResponseData();

        // Check mirror existence
        ConfigResource mirrorResource = new ConfigResource(Type.CLUSTER_MIRROR, mirrorName);
        TimelineHashMap<String, String> mirrorConfigs = configData.get(mirrorResource);
        if (mirrorConfigs == null || mirrorConfigs.isEmpty()) {
            data.setErrorCode(Errors.UNKNOWN_CLUSTER_MIRROR.code());
            data.setErrorMessage("Mirror '" + mirrorName + "' not found");
            return ControllerResult.of(records, data);
        }

        Set<ReplicationControlManager.TopicControlInfo> topicControlInfos = replicationControl.getMirrorTopics();

        // Reject if any mirror topic state changed after the broker confirmed all partitions
        // were STOPPED. A concurrent start or resume could have moved partitions out of STOPPED
        // between the broker's validation and this controller write, making the delete unsafe.
        if (stateValidationOffset >= 0) {
            for (ReplicationControlManager.TopicControlInfo topicInfo : topicControlInfos) {
                String curMirrorName = topicInfo.mirrorName();
                if (curMirrorName == null || curMirrorName.isBlank()) continue;
                if (curMirrorName.equals(mirrorName)
                        && topicInfo.lastMirrorStateChangeOffset() > stateValidationOffset) {
                    data.setErrorCode(Errors.INVALID_CLUSTER_MIRROR_STATES.code());
                    data.setErrorMessage("Mirror state for topic '" + topicInfo.name()
                            + "' changed after broker validated partition states (broker offset: "
                            + stateValidationOffset + ", last change offset: "
                            + topicInfo.lastMirrorStateChangeOffset() + ")");
                    return ControllerResult.of(records, data);
                }
            }
        }

        // Precondition: no active/paused topics
        for (ReplicationControlManager.TopicControlInfo topicInfo: topicControlInfos) {
            String curMirrorName = topicInfo.mirrorName();
            if (curMirrorName == null || curMirrorName.isBlank()) continue;
            int desiredMirrorState = topicInfo.mirrorState();
            if (curMirrorName.equals(mirrorName) && (desiredMirrorState == MirrorPartitionState.PAUSED.value() || desiredMirrorState == MirrorPartitionState.MIRRORING.value())) {
                data.setErrorCode(Errors.CLUSTER_MIRROR_NOT_EMPTY.code());
                data.setErrorMessage("Mirror '" + mirrorName + "' still has active or paused topic '"
                        + topicInfo.name() + "'");
                return ControllerResult.of(records, data);
            }
        }

        // Clear stopped topic associations
        for (ReplicationControlManager.TopicControlInfo topicInfo: topicControlInfos) {
            String curMirrorName = topicInfo.mirrorName();
            if (curMirrorName == null || curMirrorName.isBlank()) continue;
            int desiredMirrorState = topicInfo.mirrorState();
            if (curMirrorName.equals(mirrorName) && desiredMirrorState == MirrorPartitionState.STOPPED.value()) {
                records.add(new ApiMessageAndVersion(
                    new MirrorTopicStateChangeRecord()
                        .setTopicId(topicInfo.topicId())
                        .setMirrorName("")
                        .setDesiredState(MirrorPartitionState.UNKNOWN.value()),
                    (short) 0));
            }
        }

        // Tombstone the mirror config
        Map<String, Entry<OpType, String>> deleteOps = new HashMap<>();
        for (String key : mirrorConfigs.keySet()) {
            deleteOps.put(key, new AbstractMap.SimpleImmutableEntry<>(DELETE, null));
        }
        ControllerResult<ApiError> configResult = incrementalAlterConfig(mirrorResource, deleteOps, false);
        if (configResult.response().isFailure()) {
            data.setErrorCode(configResult.response().error().code());
            data.setErrorMessage("Failed to delete mirror configuration");
            return ControllerResult.of(records, data);
        }
        records.addAll(configResult.records());

        // Topic disassociations and mirror config tombstones are applied as a single atomic batch
        data.setErrorCode((short) 0);
        return ControllerResult.atomicOf(records, data);
    }

    private static Set<String> parseCsvToSet(String csv) {
        Set<String> result = new LinkedHashSet<>();
        if (csv != null && !csv.isEmpty()) {
            for (String s : csv.split(",")) {
                String trimmed = s.trim();
                if (!trimmed.isEmpty()) {
                    result.add(trimmed);
                }
            }
        }
        return result;
    }

    private ApiError updatePatternsAndStopExcluded(String mirrorName,
                                                   List<ApiMessageAndVersion> records,
                                                   Set<String> topics,
                                                   ReplicationControlManager replicationControl,
                                                   BiConsumer<Set<String>,
                                                   Set<String>> mutator) {
        ConfigResource mirrorResource = new ConfigResource(Type.CLUSTER_MIRROR, mirrorName);
        TimelineHashMap<String, String> mirrorConfigs = configData.get(mirrorResource);

        String currentInclude = mirrorConfigs != null ? mirrorConfigs.getOrDefault("mirror.topics.include", "") : "";
        String currentExclude = mirrorConfigs != null ? mirrorConfigs.getOrDefault("mirror.topics.exclude", "") : "";

        Set<String> includeSet = parseCsvToSet(currentInclude);
        Set<String> excludeSet = parseCsvToSet(currentExclude);
        mutator.accept(includeSet, excludeSet);

        ApiError topicsInPatternsError = validateTopicsInPatterns(topics, includeSet, excludeSet);
        if (topicsInPatternsError.isFailure()) {
            return topicsInPatternsError;
        }

        Map<String, Entry<OpType, String>> ops = Map.of(
                "mirror.topics.include", new AbstractMap.SimpleImmutableEntry<>(SET, String.join(",", includeSet)),
                "mirror.topics.exclude", new AbstractMap.SimpleImmutableEntry<>(SET, String.join(",", excludeSet)));
        ControllerResult<ApiError> result = incrementalAlterConfig(mirrorResource, ops, false);
        if (result.response().isFailure()) {
            return result.response();
        }
        records.addAll(result.records());
        stopExcludedTopics(mirrorName, excludeSet, records, replicationControl);
        return ApiError.NONE;
    }

    private ApiError validateTopicsInPatterns(Set<String> topics, Set<String> includeSet, Set<String> excludeSet) {
        if (topics.isEmpty()) {
            return ApiError.NONE;
        }
        Pattern includePatterns = MirrorFilterUtils.compilePatternList(includeSet.stream().toList());
        Pattern excludePatterns = MirrorFilterUtils.compilePatternList(excludeSet.stream().toList());

        if (excludePatterns != null) {
            boolean topicExcluded = topics.stream().anyMatch(topic -> excludePatterns.matcher(topic).matches());
            if (topicExcluded) {
                return new ApiError(Errors.INVALID_REQUEST, "Unable to start the topics to mirror because some topics are excluded by exclude patterns: " + excludeSet);
            }
        }
        if (includePatterns != null) {
            boolean topicNotIncluded = topics.stream().anyMatch(topic -> !includePatterns.matcher(topic).matches());
            if (topicNotIncluded) {
                return new ApiError(Errors.INVALID_REQUEST, "Unable to start the topics to mirror because some topics are not included in include patterns: " + includeSet);
            }
        }

        return ApiError.NONE;
    }

    private void stopExcludedTopics(String mirrorName, Set<String> excludePatterns, List<ApiMessageAndVersion> records, ReplicationControlManager replicationControl) {
        if (excludePatterns.isEmpty()) return;

        String combined = String.join("|", excludePatterns);
        Pattern excludePattern = Pattern.compile("^(" + combined + ")$");

        replicationControl.getMirrorTopics().forEach(topicInfo -> {
            String topicName = topicInfo.name();
            String curMirrorName = topicInfo.mirrorName();
            if (topicName == null || !curMirrorName.equals(mirrorName))
                return;

            if (excludePattern.matcher(topicName).matches()) {
                records.add(new ApiMessageAndVersion(new MirrorTopicStateChangeRecord()
                    .setTopicId(topicInfo.topicId())
                    .setMirrorName(mirrorName)
                    .setDesiredState(MirrorPartitionState.STOPPED.value()), (short) 0));
            }
        });
    }

    List<ApiMessageAndVersion> createClearElrRecordsAsNeeded(List<ApiMessageAndVersion> input) {
        if (!featureControl.isElrFeatureEnabled()) {
            return List.of();
        }
        List<ApiMessageAndVersion> output = new ArrayList<>();
        for (ApiMessageAndVersion messageAndVersion : input) {
            if (messageAndVersion.message().apiKey() == CONFIG_RECORD.id()) {
                ConfigRecord record = (ConfigRecord) messageAndVersion.message();
                if (record.name().equals(MIN_IN_SYNC_REPLICAS_CONFIG)) {
                    if (Type.forId(record.resourceType()) == Type.TOPIC) {
                        output.add(new ApiMessageAndVersion(
                            new ClearElrRecord().
                                setTopicName(record.resourceName()), (short) 0));
                    } else {
                        output.add(new ApiMessageAndVersion(new ClearElrRecord(), (short) 0));
                    }
                }
            }
        }
        return output;
    }

    ControllerResult<ApiError> incrementalAlterConfig(
        ConfigResource configResource,
        Map<String, Entry<OpType, String>> keyToOps,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        ApiError apiError = incrementalAlterConfigResource(configResource,
            keyToOps,
            newlyCreatedResource,
            outputRecords);

        outputRecords.addAll(createClearElrRecordsAsNeeded(outputRecords));
        return ControllerResult.atomicOf(outputRecords, apiError);
    }

    private ApiError incrementalAlterConfigResource(
        ConfigResource configResource,
        Map<String, Entry<OpType, String>> keysToOps,
        boolean newlyCreatedResource,
        List<ApiMessageAndVersion> outputRecords
    ) {
        List<ApiMessageAndVersion> newRecords = new ArrayList<>();
        for (Entry<String, Entry<OpType, String>> keysToOpsEntry : keysToOps.entrySet()) {
            String key = keysToOpsEntry.getKey();
            String currentValue = null;
            TimelineHashMap<String, String> currentConfigs = configData.get(configResource);
            if (currentConfigs != null) {
                currentValue = currentConfigs.get(key);
            }
            String newValue = currentValue;
            Entry<OpType, String> opTypeAndNewValue = keysToOpsEntry.getValue();
            OpType opType = opTypeAndNewValue.getKey();
            String opValue = opTypeAndNewValue.getValue();
            switch (opType) {
                case SET:
                    newValue = opValue;
                    break;
                case DELETE:
                    newValue = null;
                    break;
                case APPEND:
                case SUBTRACT:
                    if (!configSchema.isSplittable(configResource.type(), key)) {
                        return new ApiError(
                            INVALID_CONFIG, "Can't " + opType + " to " +
                            "key " + key + " because its type is not LIST.");
                    }
                    List<String> oldValueList = getParts(newValue, key, configResource);
                    if (opType == APPEND) {
                        for (String value : opValue.split(",")) {
                            if (!oldValueList.contains(value)) {
                                oldValueList.add(value);
                            }
                        }
                    } else {
                        for (String value : opValue.split(",")) {
                            oldValueList.remove(value);
                        }
                    }
                    newValue = String.join(",", oldValueList);
                    break;
            }
            if (!Objects.equals(currentValue, newValue) || configResource.type().equals(Type.BROKER)) {
                // KAFKA-14136 We need to generate records even if the value is unchanged to trigger reloads on the brokers
                newRecords.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(newValue), (short) 0));
            }
        }
        ApiError error = validateAlterConfig(configResource, newRecords, List.of(), newlyCreatedResource);
        if (error.isFailure()) {
            return error;
        }
        outputRecords.addAll(newRecords);
        return ApiError.NONE;
    }

    private ApiError validateAlterConfig(
        ConfigResource configResource,
        List<ApiMessageAndVersion> recordsExplicitlyAltered,
        List<ApiMessageAndVersion> recordsImplicitlyDeleted,
        boolean newlyCreatedResource
    ) {
        Map<String, String> allConfigs = new HashMap<>();
        Map<String, String> existingConfigsMap = new HashMap<>();
        Map<String, String> alteredConfigsForAlterConfigPolicyCheck = new HashMap<>();
        TimelineHashMap<String, String> existingConfigsSnapshot = configData.get(configResource);
        if (existingConfigsSnapshot != null) {
            allConfigs.putAll(existingConfigsSnapshot);
            existingConfigsMap.putAll(existingConfigsSnapshot);
        }
        for (ApiMessageAndVersion newRecord : recordsExplicitlyAltered) {
            ConfigRecord configRecord = (ConfigRecord) newRecord.message();
            if (isDisallowedBrokerMinIsrTransition(configRecord)) {
                return DISALLOWED_BROKER_MIN_ISR_TRANSITION_ERROR;
            } else if (isDisallowedClusterMinIsrTransition(configRecord)) {
                return DISALLOWED_CLUSTER_MIN_ISR_REMOVAL_ERROR;
            } else if (configRecord.value() == null) {
                allConfigs.remove(configRecord.name());
            } else if (configRecord.value().length() > Short.MAX_VALUE) {
                // In KRaft mode, large config values cannot be created by appending.
                // If the size exceeds Short.MAX_VALUE, this error will be thrown to notify the user.
                return DISALLOWED_CONFIG_VALUE_SIZE_ERROR;
            } else {
                allConfigs.put(configRecord.name(), configRecord.value());
            }
            alteredConfigsForAlterConfigPolicyCheck.put(configRecord.name(), configRecord.value());
        }
        for (ApiMessageAndVersion recordImplicitlyDeleted : recordsImplicitlyDeleted) {
            ConfigRecord configRecord = (ConfigRecord) recordImplicitlyDeleted.message();
            if (isDisallowedBrokerMinIsrTransition(configRecord)) {
                return DISALLOWED_BROKER_MIN_ISR_TRANSITION_ERROR;
            } else if (isDisallowedClusterMinIsrTransition(configRecord)) {
                return DISALLOWED_CLUSTER_MIN_ISR_REMOVAL_ERROR;
            } else {
                allConfigs.remove(configRecord.name());
            }
            // As per KAFKA-14195, do not include implicit deletions caused by using the legacy AlterConfigs API
            // in the list passed to the policy in order to maintain backwards compatibility
        }
        try {
            validator.validate(configResource, allConfigs, existingConfigsMap);
            if (!newlyCreatedResource) {
                existenceChecker.accept(configResource);
            }
            alterConfigPolicy.ifPresent(policy -> policy.validate(new RequestMetadata(configResource, alteredConfigsForAlterConfigPolicyCheck)));
        } catch (ConfigException e) {
            return new ApiError(INVALID_CONFIG, e.getMessage());
        } catch (Throwable e) {
            // return the corresponding API error, but emit the stack trace first if it is an unknown server error
            ApiError apiError = ApiError.fromThrowable(e);
            if (apiError.error() == Errors.UNKNOWN_SERVER_ERROR) {
                log.error("Unknown server error validating Alter Configs", e);
            }
            return apiError;
        }
        return ApiError.NONE;
    }

    private static final ApiError DISALLOWED_BROKER_MIN_ISR_TRANSITION_ERROR =
        new ApiError(INVALID_CONFIG, "Broker-level " + MIN_IN_SYNC_REPLICAS_CONFIG +
            " cannot be altered while ELR is enabled.");

    private static final ApiError DISALLOWED_CLUSTER_MIN_ISR_REMOVAL_ERROR =
        new ApiError(INVALID_CONFIG, "Cluster-level " + MIN_IN_SYNC_REPLICAS_CONFIG +
            " cannot be removed while ELR is enabled.");

    private static final ApiError DISALLOWED_CONFIG_VALUE_SIZE_ERROR =
        new ApiError(INVALID_CONFIG, "The configuration value cannot be added because " +
            "it exceeds the maximum value size of " + Short.MAX_VALUE + " bytes.");

    boolean isDisallowedBrokerMinIsrTransition(ConfigRecord configRecord) {
        if (configRecord.name().equals(MIN_IN_SYNC_REPLICAS_CONFIG) &&
                configRecord.resourceType() == BROKER.id() &&
                !configRecord.resourceName().isEmpty()) {
            if (featureControl.isElrFeatureEnabled()) {
                return true;
            }
        }
        return false;
    }

    boolean isDisallowedClusterMinIsrTransition(ConfigRecord configRecord) {
        if (configRecord.name().equals(MIN_IN_SYNC_REPLICAS_CONFIG) &&
                configRecord.resourceType() == BROKER.id() &&
                configRecord.resourceName().isEmpty() &&
                configRecord.value() == null) {
            if (featureControl.isElrFeatureEnabled()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine the result of applying a batch of legacy configuration changes.  Note
     * that this method does not change the contents of memory.  It just generates a
     * result, that you can replay later if you wish using replay().
     *
     * @param newConfigs        The new configurations to install for each resource.
     *                          All existing configurations will be overwritten.
     * @return                  The result.
     */
    ControllerResult<Map<ConfigResource, ApiError>> legacyAlterConfigs(
        Map<ConfigResource, Map<String, String>> newConfigs,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        Map<ConfigResource, ApiError> outputResults = new HashMap<>();
        for (Entry<ConfigResource, Map<String, String>> resourceEntry :
            newConfigs.entrySet()) {
            legacyAlterConfigResource(resourceEntry.getKey(),
                resourceEntry.getValue(),
                newlyCreatedResource,
                outputRecords,
                outputResults);
        }
        outputRecords.addAll(createClearElrRecordsAsNeeded(outputRecords));
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    private void legacyAlterConfigResource(ConfigResource configResource,
                                           Map<String, String> newConfigs,
                                           boolean newlyCreatedResource,
                                           List<ApiMessageAndVersion> outputRecords,
                                           Map<ConfigResource, ApiError> outputResults) {
        List<ApiMessageAndVersion> recordsExplicitlyAltered = new ArrayList<>();
        Map<String, String> currentConfigs = configData.get(configResource);
        if (currentConfigs == null) {
            currentConfigs = Map.of();
        }
        for (Entry<String, String> entry : newConfigs.entrySet()) {
            String key = entry.getKey();
            String newValue = entry.getValue();
            String currentValue = currentConfigs.get(key);
            if (!Objects.equals(currentValue, newValue) || configResource.type().equals(Type.BROKER)) {
                // KAFKA-14136 We need to generate records even if the value is unchanged to trigger reloads on the brokers
                recordsExplicitlyAltered.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(newValue), (short) 0));
            }
        }
        List<ApiMessageAndVersion> recordsImplicitlyDeleted = new ArrayList<>();
        for (String key : currentConfigs.keySet()) {
            if (!newConfigs.containsKey(key)) {
                recordsImplicitlyDeleted.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(null), (short) 0));
            }
        }
        ApiError error = validateAlterConfig(configResource, recordsExplicitlyAltered, recordsImplicitlyDeleted, newlyCreatedResource);
        if (error.isFailure()) {
            outputResults.put(configResource, error);
            return;
        }
        outputRecords.addAll(recordsExplicitlyAltered);
        outputRecords.addAll(recordsImplicitlyDeleted);
        outputResults.put(configResource, ApiError.NONE);
    }

    private List<String> getParts(String value, String key, ConfigResource configResource) {
        if (value == null) {
            value = configSchema.getDefault(configResource.type(), key);
        }
        List<String> parts = new ArrayList<>();
        if (value == null) {
            return parts;
        }
        String[] splitValues = value.split(",");
        for (String splitValue : splitValues) {
            if (!splitValue.isEmpty()) {
                parts.add(splitValue);
            }
        }
        return parts;
    }

    /**
     * Apply a configuration record to the in-memory state.
     *
     * @param record            The ConfigRecord.
     */
    public void replay(ConfigRecord record) {
        Type type = Type.forId(record.resourceType());
        ConfigResource configResource = new ConfigResource(type, record.resourceName());
        TimelineHashMap<String, String> configs = configData.get(configResource);
        if (configs == null) {
            configs = new TimelineHashMap<>(snapshotRegistry, 0);
            configData.put(configResource, configs);
            if (configResource.type().equals(BROKER) && !configResource.name().isEmpty()) {
                brokersWithConfigs.add(Integer.parseInt(configResource.name()));
            }
        }
        if (record.value() == null) {
            configs.remove(record.name());
        } else {
            configs.put(record.name(), record.value());
        }
        if (configs.isEmpty()) {
            configData.remove(configResource);
            if (configResource.type().equals(BROKER) && !configResource.name().isEmpty()) {
                brokersWithConfigs.remove(Integer.parseInt(configResource.name()));
            }
        }
        if (configSchema.isSensitive(record)) {
            log.info("Replayed ConfigRecord for {} which set configuration {} to {}",
                    configResource, record.name(), Password.HIDDEN);
        } else {
            log.info("Replayed ConfigRecord for {} which set configuration {} to {}",
                    configResource, record.name(), record.value());
        }
    }

    // VisibleForTesting
    Map<String, String> getConfigs(ConfigResource configResource) {
        Map<String, String> map = configData.get(configResource);
        if (map == null) {
            return Map.of();
        } else {
            return Map.copyOf(map);
        }
    }

    /**
     * Get the config value for the given topic and given config key.
     * The check order is:
     *   1. dynamic topic overridden configs
     *   2. dynamic node overridden configs
     *   3. dynamic cluster overridden configs
     *   4. static configs
     * If the config value is not found, return null.
     *
     * @param topicName            The topic name for the config.
     * @param configKey            The key for the config.
     * @return the config value for the provided config key in the topic
     */
    ConfigEntry getTopicConfig(String topicName, String configKey) throws NoSuchElementException {
        return configSchema.resolveEffectiveTopicConfig(configKey,
            staticConfig,
            clusterConfig(),
            currentControllerConfig(), currentTopicConfig(topicName));
    }

    public Map<ConfigResource, ResultOrError<Map<String, String>>> describeConfigs(
            long lastCommittedOffset, Map<ConfigResource, Collection<String>> resources) {
        Map<ConfigResource, ResultOrError<Map<String, String>>> results = new HashMap<>();
        for (Entry<ConfigResource, Collection<String>> resourceEntry : resources.entrySet()) {
            ConfigResource resource = resourceEntry.getKey();
            try {
                validator.validate(resource);
            } catch (Throwable e) {
                results.put(resource, new ResultOrError<>(ApiError.fromThrowable(e)));
                continue;
            }
            Map<String, String> foundConfigs = new HashMap<>();
            TimelineHashMap<String, String> configs =
                configData.get(resource, lastCommittedOffset);
            if (configs != null) {
                Collection<String> targetConfigs = resourceEntry.getValue();
                if (targetConfigs.isEmpty()) {
                    for (Entry<String, String> entry : configs.entrySet(lastCommittedOffset)) {
                        foundConfigs.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    for (String key : targetConfigs) {
                        String value = configs.get(key, lastCommittedOffset);
                        if (value != null) {
                            foundConfigs.put(key, value);
                        }
                    }
                }
            }
            results.put(resource, new ResultOrError<>(foundConfigs));
        }
        return results;
    }

    void deleteTopicConfigs(String name) {
        configData.remove(new ConfigResource(Type.TOPIC, name));
    }

    int getStaticallyConfiguredMinInsyncReplicas() {
        return configSchema.getStaticallyConfiguredMinInsyncReplicas(staticConfig);
    }

    /**
     * Generate any configuration records that are needed to make it safe to enable ELR.
     * Specifically, we need to remove all broker-level configurations for min.insync.replicas,
     * and create a cluster-level configuration for min.insync.replicas. It is always safe to call
     * this function if ELR is already enabled; it will simply do nothing if the necessary
     * configurations already exist.
     *
     * @param outputRecords     A list to add the new records to.
     *
     * @return                  The log message to generate.
     */
    String maybeGenerateElrSafetyRecords(List<ApiMessageAndVersion> outputRecords) {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        if (!clusterConfig().containsKey(MIN_IN_SYNC_REPLICAS_CONFIG)) {
            int minInsyncReplicas = configSchema.getStaticallyConfiguredMinInsyncReplicas(staticConfig);
            outputRecords.add(new ApiMessageAndVersion(
                new ConfigRecord().
                    setResourceType(BROKER.id()).
                    setResourceName("").
                    setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
                    setValue(Integer.toString(minInsyncReplicas)),
                CONFIG_RECORD.highestSupportedVersion()));
            bld.append("Generating cluster-level ").append(MIN_IN_SYNC_REPLICAS_CONFIG).
                append(" of ").append(minInsyncReplicas);
            prefix = ". ";
        }
        prefix = prefix + "Removing broker-level " + MIN_IN_SYNC_REPLICAS_CONFIG + " for brokers: ";
        for (Integer brokerId : brokersWithConfigs) {
            ConfigResource configResource = new ConfigResource(BROKER, brokerId.toString());
            Map<String, String> configs = configData.get(configResource);
            if (configs.containsKey(MIN_IN_SYNC_REPLICAS_CONFIG)) {
                outputRecords.add(new ApiMessageAndVersion(
                    new ConfigRecord().setResourceType(BROKER.id()).setResourceName(configResource.name()).
                        setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).setValue(null),
                    CONFIG_RECORD.highestSupportedVersion()));
                bld.append(prefix).append(brokerId);
                prefix = ", ";
            }
        }
        if (bld.isEmpty()) {
            return "";
        } else {
            bld.append(".");
            return bld.toString();
        }
    }

    /**
     * Update a Kafka feature, generating any configuration changes that are required.
     *
     * @param updates       The user-requested updates.
     * @param upgradeTypes  The user-requested upgrade types.
     * @param validateOnly  True if we should validate the request but not make changes.
     * @param currentClaimEpoch the currently claimed epoch
     *
     * @return              The result.
     */
    ControllerResult<ApiError> updateFeatures(
        Map<String, Short> updates,
        Map<String, FeatureUpdate.UpgradeType> upgradeTypes,
        boolean validateOnly,
        int currentClaimEpoch
    ) {
        ControllerResult<ApiError> result = featureControl.updateFeatures(updates, upgradeTypes, validateOnly, currentClaimEpoch);
        if (result.response().isSuccess() &&
            !validateOnly &&
            updates.getOrDefault(EligibleLeaderReplicasVersion.FEATURE_NAME, (short) 0) > 0
        ) {
            List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
            String logMessage = maybeGenerateElrSafetyRecords(records);
            if (!logMessage.isEmpty()) {
                log.info("{}", logMessage);
            }
            records.addAll(result.records());
            return ControllerResult.atomicOf(records, ApiError.NONE);
        }
        return result;
    }

    /**
     * Check if this topic has "unclean.leader.election.enable" set to true.
     *
     * @param topicName            The topic name for the config.
     * @return true if this topic has uncleanLeaderElection enabled
     */
    boolean uncleanLeaderElectionEnabledForTopic(String topicName) {
        String uncleanLeaderElection = getTopicConfig(topicName, UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG).value();
        if (!uncleanLeaderElection.isEmpty()) {
            return Boolean.parseBoolean(uncleanLeaderElection);
        }
        return false;
    }

    Map<String, ConfigEntry> computeEffectiveTopicConfigs(Map<String, String> creationConfigs) {
        return configSchema.resolveEffectiveTopicConfigs(staticConfig, clusterConfig(),
            currentControllerConfig(), creationConfigs);
    }

    Map<String, String> clusterConfig() {
        Map<String, String> result = configData.get(DEFAULT_NODE);
        return (result == null) ? Map.of() : result;
    }

    Map<String, String> currentControllerConfig() {
        Map<String, String> result = configData.get(currentController);
        return (result == null) ? Map.of() : result;
    }

    Map<String, String> currentTopicConfig(String topicName) {
        Map<String, String> result = configData.get(new ConfigResource(Type.TOPIC, topicName));
        return (result == null) ? Map.of() : result;
    }

    // Visible to test
    TimelineHashSet<Integer> brokersWithConfigs() {
        return brokersWithConfigs;
    }
}
