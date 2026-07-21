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
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AlterDisklessSwitchRequestData;
import org.apache.kafka.common.message.AlterDisklessSwitchResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.InitDisklessLogRequestData;
import org.apache.kafka.common.message.InitDisklessLogResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.controller.ReplicationControlManagerTest.ReplicationControlTestContext;
import org.apache.kafka.metadata.InitDisklessLogFields;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.kafka.common.config.TopicConfig.DISKLESS_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
import static org.apache.kafka.common.protocol.Errors.FENCED_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.Errors.INVALID_REPLICATION_FACTOR;
import static org.apache.kafka.common.protocol.Errors.INVALID_REPLICA_ASSIGNMENT;
import static org.apache.kafka.common.protocol.Errors.INVALID_REQUEST;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.NOT_CONTROLLER;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_ID;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.apache.kafka.controller.ReplicationControlManagerTest.NONE_REASSIGNING;
import static org.apache.kafka.controller.ReplicationControlManagerTest.defaultBrokerEpoch;
import static org.apache.kafka.controller.ReplicationControlManagerTest.withoutConfigs;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class ReplicationControlManagerInklessTest {
    @Nested
    // Tests Diskless single/unmanaged replica approach where a single replica is registered on KRaft but it's effectively ignored.
    class DisklessUnmanagedReplicaTests {
        @ParameterizedTest
        @CsvSource({
            "false,false",
            "false,",
            "true,false",
        })
        public void testNotCreateDisklessTopic(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given a request to create a kafka topic with diskless disabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection creatableTopicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                creatableTopicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }

            request.topics().add(new CreatableTopic()
                .setName("foo")
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1)
                .setConfigs(creatableTopicConfigs));

            // Given all brokers unfenced
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
            // Then the topic creation should succeed, regardless of the RF
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            expectedResponse.topics().add(new CreatableTopicResult()
                .setName("foo")
                .setNumPartitions(1)
                .setReplicationFactor((short) 3)
                .setErrorMessage(null)
                .setErrorCode((short) 0)
                .setTopicId(result.response().topics().find("foo").topicId()));
            final List<CreateTopicsResponseData.CreatableTopicConfigs> disklessTopicConfigs = result.response().topics().find("foo").configs().stream()
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            assertTrue(disklessTopicConfigs.isEmpty() || disklessTopicConfigs.stream().allMatch(c -> c.value().equals("false")));
            assertEquals(expectedResponse, withoutConfigs(result.response()));
            final List<ConfigRecord> disklessConfigRecords = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            // If diskless.enable is explicitly set, it's normalized to false; if omitted, no record is emitted.
            assertTrue(disklessConfigRecords.isEmpty() || disklessConfigRecords.stream().allMatch(c -> c.value().equals("false")));

            // Given the topic is registered
            ctx.replay(result.records());
            assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 0})
                    .setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00001DIRAAAA"),
                        Uuid.fromString("TESTBROKER00002DIRAAAA"),
                        Uuid.fromString("TESTBROKER00000DIRAAAA")
                    })
                    .setIsr(new int[] {1, 2, 0})
                    .setLeader(1)
                    .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                    .setLeaderEpoch(0)
                    .setPartitionEpoch(0)
                    .build(),
                replicationControl.getPartition(((TopicRecord) result.records().get(0).message()).topicId(), 0));

            // When creating a topic with diskless enabled and already exists
            ControllerResult<CreateTopicsResponseData> result1 =
                replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
            CreateTopicsResponseData expectedResponse1 = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse1.topics().add(new CreatableTopicResult().setName("foo")
                .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                .setErrorMessage("Topic 'foo' already exists."));
            assertEquals(expectedResponse1, result1.response());
        }

        @ParameterizedTest
        @CsvSource(value = {
            "true,true",
            "true,NULL",
            "false,true",
        }, nullValues = "NULL")
        public void testCreateDisklessTopic_noRacks(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given a request to create a kafka topic with diskless enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection creatableTopicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                creatableTopicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }
            request.topics().add(new CreatableTopic()
                .setName("foo")
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1)
                .setConfigs(creatableTopicConfigs));

            // When creating a topic without brokers available
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should fail with BROKER_NOT_AVAILABLE error
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            expectedResponse.topics().add(new CreatableTopicResult().setName("foo")
                .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
                .setErrorMessage("No brokers available to create diskless topic."));
            assertEquals(expectedResponse, withoutConfigs(result.response()));

            // Given brokers are registered
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result2 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should succeed, regardless of fenced brokers
            CreateTopicsResponseData expectedResponse2 = new CreateTopicsResponseData();
            expectedResponse2.topics().add(new CreatableTopicResult()
                .setName("foo")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1)
                .setErrorMessage(null)
                .setErrorCode((short) 0)
                .setTopicId(result2.response().topics().find("foo").topicId()));
            CreateTopicsResponseData response = result2.response();
            assertEquals(expectedResponse2, withoutConfigs(response));

            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result3 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should succeed, regardless of the RF
            CreateTopicsResponseData expectedResponse3 = new CreateTopicsResponseData();
            expectedResponse3.topics().add(new CreatableTopicResult()
                .setName("foo")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1)
                .setErrorMessage(null)
                .setErrorCode((short) 0)
                .setTopicId(result3.response().topics().find("foo").topicId()));
            assertEquals(expectedResponse3, withoutConfigs(result3.response()));
            final List<ConfigRecord> disklessConfigRecords = result3.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            assertEquals(1, disklessConfigRecords.size());
            // Then diskless is always enabled
            assertTrue(disklessConfigRecords.stream().allMatch(c -> c.value().equals("true")));

            // Given the topic is registered
            ctx.replay(result3.records());
            assertEquals(
                new PartitionRegistration.Builder().setReplicas(new int[] {0})
                    .setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00000DIRAAAA"),
                    })
                    .setIsr(new int[] {0})
                    .setLeader(0)
                    .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                    .setLeaderEpoch(0)
                    .setPartitionEpoch(0)
                    .build(),
                replicationControl.getPartition(((TopicRecord) result3.records().get(0).message()).topicId(), 0));

            // When creating a topic with diskless enabled and already exists
            ControllerResult<CreateTopicsResponseData> result4 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            CreateTopicsResponseData expectedResponse4 = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse4.topics().add(new CreatableTopicResult().setName("foo")
                .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                .setErrorMessage("Topic 'foo' already exists."));
            assertEquals(expectedResponse4, result4.response());
        }

        @ParameterizedTest
        @CsvSource(value = {
            "true,true",
            "true,NULL",
            "false,true",
        }, nullValues = "NULL")
        public void testCreateDisklessTopic_withRacks(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given a request to create a kafka topic with diskless enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection creatableTopicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                creatableTopicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }
            request.topics().add(new CreatableTopic()
                .setName("foo")
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1)
                .setConfigs(creatableTopicConfigs));

            // When creating a topic without brokers available
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should fail with BROKER_NOT_AVAILABLE error
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            expectedResponse.topics().add(new CreatableTopicResult().setName("foo")
                .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
                .setErrorMessage("No brokers available to create diskless topic."));
            assertEquals(expectedResponse, withoutConfigs(result.response()));

            // Given brokers are registered
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result2 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should succeed, regardless of fenced brokers
            CreateTopicsResponseData expectedResponse2 = new CreateTopicsResponseData();
            expectedResponse2.topics().add(new CreatableTopicResult()
                .setName("foo")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1)
                .setErrorMessage(null)
                .setErrorCode((short) 0)
                .setTopicId(result2.response().topics().find("foo").topicId()));
            CreateTopicsResponseData response = result2.response();
            assertEquals(expectedResponse2, withoutConfigs(response));

            // Given all brokers unfenced
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result3 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should succeed, regardless of the RF
            CreateTopicsResponseData expectedResponse3 = new CreateTopicsResponseData();
            expectedResponse3.topics().add(new CreatableTopicResult()
                .setName("foo")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1)
                .setErrorMessage(null)
                .setErrorCode((short) 0)
                .setTopicId(result3.response().topics().find("foo").topicId()));
            assertEquals(expectedResponse3, withoutConfigs(result3.response()));
            final List<ConfigRecord> disklessConfigRecords = result3.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            assertEquals(1, disklessConfigRecords.size());
            // Then diskless is always enabled
            assertTrue(disklessConfigRecords.stream().allMatch(c -> c.value().equals("true")));

            // Given the topic is registered
            ctx.replay(result3.records());
            assertEquals(
                new PartitionRegistration.Builder().setReplicas(new int[] {0})
                    .setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00000DIRAAAA"),
                    })
                    .setIsr(new int[] {0})
                    .setLeader(0)
                    .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                    .setLeaderEpoch(0)
                    .setPartitionEpoch(0)
                    .build(),
                replicationControl.getPartition(((TopicRecord) result3.records().get(0).message()).topicId(), 0));

            // When creating a topic with diskless enabled and already exists
            ControllerResult<CreateTopicsResponseData> result4 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            CreateTopicsResponseData expectedResponse4 = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse4.topics().add(new CreatableTopicResult().setName("foo")
                .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                .setErrorMessage("Topic 'foo' already exists."));
            assertEquals(expectedResponse4, result4.response());
        }

        @ParameterizedTest
        @CsvSource({
            "1, -2, INVALID_REPLICATION_FACTOR",
            "1, 0, INVALID_REPLICATION_FACTOR",
            "1, 2, INVALID_REPLICATION_FACTOR",
            "-2, 1, INVALID_PARTITIONS",
            "0, 1, INVALID_PARTITIONS",
        })
        public void testCreateDisklessTopicWithInvalidInput(int numPartitions, short replicationFactor, String expectedError) {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);

            CreateTopicsRequestData.CreatableTopicConfigCollection disklessConfig =
                new CreateTopicsRequestData.CreatableTopicConfigCollection();
            disklessConfig.add(
                new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName("diskless.enable")
                    .setValue("true")
            );

            CreateTopicsRequestData request1 = new CreateTopicsRequestData();
            request1.topics().add(new CreatableTopic().setName("baz")
                .setNumPartitions(numPartitions).setReplicationFactor(replicationFactor)
                .setConfigs(disklessConfig));

            ControllerResult<CreateTopicsResponseData> result1 =
                replicationControl.createTopics(requestContext, request1, Set.of("baz"));
            assertEquals(Errors.valueOf(expectedError).code(), result1.response().topics().find("baz").errorCode());
            assertEquals(List.of(), result1.records());
        }

        @ParameterizedTest
        @CsvSource(value = {
            "true,false",
            "true,NULL"
            // This case is not valid because no internal topic should be explicitly created with diskless enabled.
            // Tested in testInvalidDisklessTopicCreationForInternalTopics
            // "false,true",
        }, nullValues = "NULL")
        public void testCreateInternalTopicWithDisklessEnabled(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            // Given a setup with diskless defined at the server level
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given an internal kafka topic with diskless enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection creatableTopicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                // If the diskless enable config is set, it should be added to the topic configs
                creatableTopicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }
            final String internalTopic = Topic.GROUP_METADATA_TOPIC_NAME;
            request.topics().add(new CreatableTopic().setName(internalTopic).
                setNumPartitions(-1).setReplicationFactor((short) -1)
                .setConfigs(creatableTopicConfigs));
            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            // When creating an internal topic with diskless enabled, disable it
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of(internalTopic));
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse.topics().add(
                new CreatableTopicResult()
                    .setName(internalTopic)
                    .setNumPartitions(1)
                    .setReplicationFactor((short) 3)
                    .setErrorMessage(null).setErrorCode((short) 0)
                    .setTopicId(result.response().topics().find(internalTopic).topicId()));
            assertEquals(expectedResponse, withoutConfigs(result.response()));
            assertTrue(result.response().topics().find(internalTopic)
                .configs()
                .stream()
                .noneMatch(c -> c.name().equals(DISKLESS_ENABLE_CONFIG)));
            final List<ConfigRecord> disklessConfigRecords = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            // Then always diskless is explicitly disabled
            assertFalse(disklessConfigRecords.isEmpty(),
                "Expected explicit diskless.enable=false ConfigRecord for internal topic");
            assertTrue(disklessConfigRecords.stream().allMatch(c -> c.value().equals("false")));
        }

        @ParameterizedTest
        @ValueSource(strings = {"__remote_log_metadata", "__cluster_metadata"})
        public void testCreateSystemTopicAsClassicWhenDisklessEnabled(String systemTopic) {
            // Given a setup with diskless enabled at the server level
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(true)
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given a system topic creation request without explicit diskless config
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            request.topics().add(
                new CreatableTopic()
                    .setName(systemTopic)
                    .setNumPartitions(1)
                    .setReplicationFactor((short) 3));
            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            // When creating the system topic
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of(systemTopic));
            // Then the topic creation should succeed
            assertEquals(Errors.NONE.code(), result.response().topics().find(systemTopic).errorCode());
            // And diskless should not be enabled
            List<ConfigRecord> disklessConfigRecords = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            assertFalse(disklessConfigRecords.isEmpty(),
                "Expected explicit diskless.enable=false ConfigRecord for system topic");
            assertTrue(disklessConfigRecords.stream().allMatch(c -> c.value().equals("false")));
        }

        @ParameterizedTest
        @ValueSource(strings = {"__remote_log_metadata", "__cluster_metadata"})
        public void testRejectExplicitDisklessEnableForSystemTopics(String systemTopic) {
            // Given a setup with the diskless storage system enabled
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given a system topic creation request with diskless explicitly enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection topicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            topicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(DISKLESS_ENABLE_CONFIG)
                .setValue("true"));
            request.topics().add(
                new CreatableTopic()
                    .setName(systemTopic)
                    .setNumPartitions(1)
                    .setReplicationFactor((short) 3)
                    .setConfigs(topicConfigs));
            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            // When creating the system topic with diskless explicitly enabled
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of(systemTopic));
            // Then the topic creation should be rejected
            CreatableTopicResult topicResult = result.response().topics().find(systemTopic);
            assertEquals(Errors.INVALID_REQUEST.code(), topicResult.errorCode());
            assertEquals("System topics cannot be diskless topics.", topicResult.errorMessage());
        }

        @ParameterizedTest
        @ValueSource(strings = {"__remote_log_metadata", "__cluster_metadata"})
        public void testRejectAlterConfigDisklessEnableForSystemTopics(String systemTopic) {
            // Given a setup with the diskless storage system enabled and allow-from-classic enabled
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            // Given a system topic already exists
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic(systemTopic, new int[][] {new int[] {0, 1, 2}});
            // When attempting to alter diskless.enable to true on the system topic
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, systemTopic);
            ControllerResult<Map<ConfigResource, ApiError>> result =
                ctx.configurationControl.incrementalAlterConfigs(
                    Map.of(resource, Map.of(DISKLESS_ENABLE_CONFIG,
                        new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"))),
                    false);
            // Then the alter config should be rejected
            assertEquals(Errors.INVALID_CONFIG.code(), result.response().get(resource).error().code());
            assertTrue(result.response().get(resource).message().contains("System topics cannot be diskless"));
        }

        @ParameterizedTest
        @ValueSource(strings = {"__remote_log_metadata", "__cluster_metadata"})
        public void testRejectLegacyAlterConfigDisklessEnableForSystemTopics(String systemTopic) {
            // Given a setup with the diskless storage system enabled and allow-from-classic enabled
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            // Given a system topic already exists
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic(systemTopic, new int[][] {new int[] {0, 1, 2}});
            // When attempting to set diskless.enable=true via legacy AlterConfigs
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, systemTopic);
            ControllerResult<Map<ConfigResource, ApiError>> result =
                ctx.configurationControl.legacyAlterConfigs(
                    Map.of(resource, Map.of(DISKLESS_ENABLE_CONFIG, "true")),
                    false);
            // Then the alter config should be rejected
            assertEquals(Errors.INVALID_CONFIG.code(), result.response().get(resource).error().code());
            assertTrue(result.response().get(resource).message().contains("System topics cannot be diskless"));
        }

        @Test
        public void testInvalidDisklessTopicCreationForInternalTopics() {
            // Given a setup with diskless defined at the server level
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given an internal kafka topic with diskless enabled
            final String internalTopic = Topic.GROUP_METADATA_TOPIC_NAME;
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection topicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            topicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(DISKLESS_ENABLE_CONFIG)
                .setValue("true"));
            request.topics().add(new CreatableTopic().setName(internalTopic).setConfigs(topicConfigs));
            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            // When creating an internal topic with diskless enabled, disable it
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of(internalTopic));
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse.topics().add(
                new CreatableTopicResult()
                    .setName(internalTopic)
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("System topics cannot be diskless topics."));
            assertEquals(expectedResponse, withoutConfigs(result.response()));
        }

        @ParameterizedTest
        @CsvSource(value = {
            "false,true",
            "true,NULL"
        }, nullValues = "NULL")
        public void testInvalidDisklessTopicCreationWithoutSystemEnabled(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            // Given a setup with diskless defined at the server level
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(false)
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given an internal kafka topic with diskless enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection topicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                // If the diskless enable config is set, it should be added to the topic configs
                topicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }
            final String topicName = "foo";
            request.topics().add(new CreatableTopic().setName(topicName).setConfigs(topicConfigs));
            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            // When creating an internal topic with diskless enabled, disable it
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of(topicName));
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse.topics().add(
                new CreatableTopicResult()
                    .setName(topicName)
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Cannot create diskless topics when the diskless storage system is disabled. Please enable the diskless storage system to create diskless topics."));
            assertEquals(expectedResponse, withoutConfigs(result.response()));
        }

        @Test
        public void testCreateTopicWithDisklessExplicitlyDisabledWhenSystemDisabledCreatesTieredTopic() {
            // Given diskless storage system is globally disabled
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(false)
                .setDefaultDisklessEnable(false)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;

            // Given a topic creation request with diskless.enable=false explicitly
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection topicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            topicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(DISKLESS_ENABLE_CONFIG)
                .setValue("false"));
            request.topics().add(new CreatableTopic().setName("foo")
                .setNumPartitions(-1).setReplicationFactor((short) -1)
                .setConfigs(topicConfigs));

            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // When creating the topic
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));

            // Then the topic creation should succeed by enabling tiered storage instead of diskless
            CreatableTopicResult topicResult = result.response().topics().find("foo");
            assertNotNull(topicResult);
            assertEquals(NONE.code(), topicResult.errorCode(),
                "Topic creation should succeed as a tiered topic when diskless is explicitly disabled and system is disabled");

            // And it should be created as a tiered topic (remote.storage.enable=true)
            List<ConfigRecord> remoteStorageConfigRecords = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(REMOTE_LOG_STORAGE_ENABLE_CONFIG))
                .toList();
            assertFalse(remoteStorageConfigRecords.isEmpty(),
                "Expected remote.storage.enable config record for tiered topic");
            assertTrue(remoteStorageConfigRecords.stream().allMatch(c -> c.value().equals("true")),
                "Expected remote.storage.enable=true for tiered topic");

            // And diskless.enable should not be persisted (it was stripped)
            List<ConfigRecord> disklessConfigRecords = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            assertTrue(disklessConfigRecords.isEmpty(),
                    "Expected no diskless.enable config record when diskless system is disabled");
        }

        @Test
        public void testReassignDisklessPartitions() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1);
            ctx.unfenceBrokers(0, 1);

            String topic = "foo";
            CreatableTopicResult createResult = ctx.createTestTopic(
                topic,
                1,
                (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );

            // No change in the replication factor.
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult1 =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName(topic).setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1)))))));
            assertEquals(new AlterPartitionReassignmentsResponseData().
                    setErrorMessage(null).setResponses(List.of(
                        new ReassignableTopicResponse().setName(topic).setPartitions(List.of(
                            new ReassignablePartitionResponse().setPartitionIndex(0).setErrorMessage(null))))),
                alterResult1.response());

            ctx.replay(alterResult1.records());

            // For diskless topics, reassignment completes immediately.
            // There should be no ongoing reassignment.
            assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(List.of(
                new ListPartitionReassignmentsTopics().setName(topic).
                    setPartitionIndexes(List.of(0))), Long.MAX_VALUE));

            // Verify the partition now has the new replica (reassignment completed immediately)
            PartitionRegistration partition = replication.getPartition(createResult.topicId(), 0);
            assertEquals(List.of(1), Replicas.toList(partition.replicas));
            // ISR must match replicas — diskless brokers are immediately in-sync via object storage
            assertEquals(List.of(1), Replicas.toList(partition.isr));

            // Try to increase the replication factor (should fail for diskless).
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult2 =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName(topic).setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0)
                                .setReplicas(List.of(0, 1)))))));
            assertEquals(new AlterPartitionReassignmentsResponseData()
                    .setErrorMessage(null).setResponses(List.of(
                        new ReassignableTopicResponse().setName(topic).setPartitions(List.of(
                            new ReassignablePartitionResponse().setPartitionIndex(0)
                                .setErrorCode(INVALID_REPLICATION_FACTOR.code())
                                .setErrorMessage("The replication factor is changed from 1 to 2"))))),
                alterResult2.response());
        }

        @Test
        public void testReassignDisklessPartitionsToAllFencedBrokersIsRejected() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1);
            ctx.unfenceBrokers(0, 1);

            String topic = "foo";
            ctx.createTestTopic(
                topic, 1, (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), NONE.code());

            // Fence broker 1
            ctx.fenceBrokers(1);

            // Reassign to only the fenced broker — should be rejected
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName(topic).setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1)))))));
            assertEquals(new AlterPartitionReassignmentsResponseData()
                    .setErrorMessage(null).setResponses(List.of(
                        new ReassignableTopicResponse().setName(topic).setPartitions(List.of(
                            new ReassignablePartitionResponse().setPartitionIndex(0)
                                .setErrorCode(INVALID_REPLICA_ASSIGNMENT.code())
                                .setErrorMessage("None of the target replicas [1] are active."))))),
                alterResult.response());
        }

        @Test
        public void testNoLeaderElectionOnBrokerFenced_noRacks() {
            // As there are no replicas to elect from, the leader should go offline but no new leader should be elected.
            // Unmanaged diskless topics register a single replica as the leader, but it's not maintained.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                1,
                (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            // Get the actual leader before fencing
            PartitionRegistration partitionBefore = replication.getPartition(topicId, 0);
            int leader = partitionBefore.leader;

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerFenced(leader, records);
            ctx.replay(records);

            PartitionRegistration partition = replication.getPartition(topicId, 0);
            assertNotNull(partition, "Partition should exist after leader fencing");
            assertArrayEquals(new int[]{leader}, partition.isr, "ISR should remain unchanged as there is only one replica");
            assertEquals(-1, partition.leader, "Leader should be offline after fencing");
        }

        @Test
        public void testNoLeaderElectionOnBrokerFenced_withRacks() {
            // As there are no replicas to elect from, the leader should go offline but no new leader should be elected.
            // Unmanaged diskless topics register a single replica as the leader, but it's not maintained.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                1,
                (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            // Get the actual leader before fencing
            PartitionRegistration partitionBefore = replication.getPartition(topicId, 0);
            int leader = partitionBefore.leader;

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerFenced(leader, records);
            ctx.replay(records);

            PartitionRegistration partition = replication.getPartition(topicId, 0);
            assertNotNull(partition, "Partition should exist after leader fencing");
            assertArrayEquals(new int[]{leader}, partition.isr, "ISR should remain unchanged as there is only one replica");
            assertEquals(-1, partition.leader, "Leader should be offline after fencing");
        }

        @Test
        public void testNoReplicaChangeOnShutdown_noRacks() {
            // As there are no replicas to elect from, the leader should go offline but no new leader should be elected.
            // Unmanaged diskless topics register a single replica as the leader, but it's not maintained.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                1,
                (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            // Get the actual leader before shutdown
            PartitionRegistration partitionBefore = replication.getPartition(topicId, 0);
            int leader = partitionBefore.leader;

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerShutdown(leader, true, records);
            ctx.replay(records);

            PartitionRegistration partition = replication.getPartition(topicId, 0);
            assertNotNull(partition, "Partition should exist after leader shutdown");
            assertArrayEquals(new int[]{leader}, partition.isr, "ISR should remain unchanged as there is only one replica");
            assertEquals(-1, partition.leader, "Leader should be offline after shutdown");
        }

        @Test
        public void testNoReplicaChangeOnShutdown_withRacks() {
            // As there are no replicas to elect from, the leader should go offline but no new leader should be elected.
            // Unmanaged diskless topics register a single replica as the leader, but it's not maintained.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                1,
                (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            // Get the actual leader before shutdown
            PartitionRegistration partitionBefore = replication.getPartition(topicId, 0);
            int leader = partitionBefore.leader;

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerShutdown(leader, true, records);
            ctx.replay(records);

            PartitionRegistration partition = replication.getPartition(topicId, 0);
            assertNotNull(partition, "Partition should exist after leader shutdown");
            assertArrayEquals(new int[]{leader}, partition.isr, "ISR should remain unchanged as there is only one replica");
            assertEquals(-1, partition.leader, "Leader should be offline after shutdown");
        }

        @Test
        void testDisklessMarksLeaderOfflineOnUnregister_noRacks() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            final int numPartitions = 6;
            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                numPartitions,
                (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            // Identify partitions that have broker 0 as leader before unregistering
            Set<Integer> partitionsWithBroker0AsLeader = new HashSet<>();
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                PartitionRegistration partition = replication.getPartition(topicId, partitionId);
                if (partition.leader == 0) {
                    partitionsWithBroker0AsLeader.add(partitionId);
                }
            }

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerUnregistered(0, 100, records);
            ctx.replay(records);

            // All partitions should remain present and keep the original replica/ISR,
            // only leaders on broker 0 should be marked offline.
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                PartitionRegistration partition = replication.getPartition(topicId, partitionId);
                assertNotNull(partition, "Partition " + partitionId + " should exist after broker unregistration");
                assertEquals(1, partition.replicas.length, "Replicas should have 1 element for partition " + partitionId);
                assertEquals(1, partition.isr.length, "ISR should have 1 element for partition " + partitionId);
                if (partitionsWithBroker0AsLeader.contains(partitionId)) {
                    assertEquals(-1, partition.leader, "Leader should be offline for partition " + partitionId + " (was on broker 0)");
                } else {
                    assertTrue(partition.leader >= 0, "Leader should remain online for partition " + partitionId + " (was not on broker 0)");
                }
            }
        }

        @Test
        void testDisklessMarksLeaderOfflineOnUnregister_withRacks() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            final int numPartitions = 6;
            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                numPartitions,
                (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            // Identify partitions that have broker 0 as leader before unregistering
            Set<Integer> partitionsWithBroker0AsLeader = new HashSet<>();
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                PartitionRegistration partition = replication.getPartition(topicId, partitionId);
                if (partition.leader == 0) {
                    partitionsWithBroker0AsLeader.add(partitionId);
                }
            }

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerUnregistered(0, 100, records);
            ctx.replay(records);

            // All partitions should remain present and keep the original replica/ISR,
            // only leaders on broker 0 should be marked offline.
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                PartitionRegistration partition = replication.getPartition(topicId, partitionId);
                assertNotNull(partition, "Partition " + partitionId + " should exist after broker unregistration");
                assertEquals(1, partition.replicas.length, "Replicas should have 1 element for partition " + partitionId);
                assertEquals(1, partition.isr.length, "ISR should have 1 element for partition " + partitionId);
                if (partitionsWithBroker0AsLeader.contains(partitionId)) {
                    assertEquals(-1, partition.leader, "Leader should be offline for partition " + partitionId + " (was on broker 0)");
                } else {
                    assertTrue(partition.leader >= 0, "Leader should remain online for partition " + partitionId + " (was not on broker 0)");
                }
            }
        }

        @Test
        void testManualReplicaAssignmentsShouldBeRejected() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Expectation: providing manual replica assignments for a diskless topic should be rejected.
            ctx.createTestTopic(
                "foo",
                new int[][] {new int[] {0, 1}, new int[] {1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                INVALID_REQUEST.code()
            );
        }

        @Test
        public void testAddPartitionsAutoPlacement() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1);
            ctx.unfenceBrokers(0, 1);

            // Create a diskless topic with RF=1 (unmanaged), 1 partition
            String topic = "foo";
            CreatableTopicResult createResult = ctx.createTestTopic(
                topic, 1, (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), NONE.code());

            // Add 2 more partitions (auto-placement, no manual assignments)
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
            ControllerResult<List<CreatePartitionsTopicResult>> addResult =
                replication.createPartitions(requestContext, List.of(
                    new CreatePartitionsTopic().setName(topic).setCount(3).setAssignments(null)));
            assertEquals(NONE.code(), addResult.response().get(0).errorCode());
            ctx.replay(addResult.records());

            // Verify new partitions have RF=1 (inherited from existing partition)
            for (int p = 0; p < 3; p++) {
                PartitionRegistration partition = replication.getPartition(createResult.topicId(), p);
                assertNotNull(partition, "Partition " + p + " should exist");
                assertEquals(1, partition.replicas.length,
                    "Partition " + p + " should have RF=1");
                assertTrue(partition.leader >= 0,
                    "Partition " + p + " should have a valid leader");
            }
        }

        @Test
        public void testAddPartitionsWithFencedBroker() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Create a diskless topic with RF=1 (unmanaged), 1 partition
            String topic = "foo";
            CreatableTopicResult createResult = ctx.createTestTopic(
                topic, 1, (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), NONE.code());

            // Fence broker 2
            ctx.fenceBrokers(2);

            // Add 2 more partitions — should succeed, new partitions placed on unfenced brokers
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
            ControllerResult<List<CreatePartitionsTopicResult>> addResult =
                replication.createPartitions(requestContext, List.of(
                    new CreatePartitionsTopic().setName(topic).setCount(3).setAssignments(null)));
            assertEquals(NONE.code(), addResult.response().get(0).errorCode());
            ctx.replay(addResult.records());

            // Verify new partitions are placed on unfenced brokers
            for (int p = 1; p < 3; p++) {
                PartitionRegistration partition = replication.getPartition(createResult.topicId(), p);
                assertNotNull(partition, "Partition " + p + " should exist");
                assertEquals(1, partition.replicas.length,
                    "Partition " + p + " should have RF=1");
                assertNotEquals(2, partition.replicas[0],
                    "Partition " + p + " should not be placed on fenced broker");
                assertTrue(partition.leader >= 0,
                    "Partition " + p + " should have a valid leader");
            }
        }

        @Test
        public void testAddPartitionsManualAssignmentRejectedForUnmanaged() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1);
            ctx.unfenceBrokers(0, 1);

            // Create a diskless topic with RF=1 (unmanaged), 1 partition
            String topic = "foo";
            ctx.createTestTopic(topic, 1, (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), NONE.code());

            // Try to add 1 partition with manual assignment — should be rejected
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
            ControllerResult<List<CreatePartitionsTopicResult>> addResult =
                replication.createPartitions(requestContext, List.of(
                    new CreatePartitionsTopic().setName(topic).setCount(2).setAssignments(List.of(
                        new CreatePartitionsAssignment().setBrokerIds(List.of(1))))));
            assertEquals(INVALID_REPLICA_ASSIGNMENT.code(), addResult.response().get(0).errorCode());
            assertEquals("A manual partition assignment cannot be specified for diskless topics.",
                addResult.response().get(0).errorMessage());
        }
    }

    @Nested
    // Tests Diskless managed-replicas
    class DisklessManagedReplicasTests {
        @ParameterizedTest
        @CsvSource(value = {
            "false,false",
            "false,NULL",
            "true,false",
        }, nullValues = "NULL")
        public void testCreatesClassicTopicWhenDisklessDisabled(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given a request to create a kafka topic with diskless disabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection creatableTopicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                creatableTopicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }

            request.topics().add(new CreatableTopic().setName("foo").
                setNumPartitions(-1).setReplicationFactor((short) -1)
                .setConfigs(creatableTopicConfigs));

            // Given all brokers unfenced
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // When creating a topic with diskless disabled
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
            // Then the topic creation should succeed as a classic topic with RF=3
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
                setNumPartitions(1).setReplicationFactor((short) 3).
                setErrorMessage(null).setErrorCode((short) 0).
                setTopicId(result.response().topics().find("foo").topicId()));
            assertEquals(expectedResponse, withoutConfigs(result.response()));
            final List<ConfigRecord> disklessConfigRecords = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            if (!disklessConfigRecords.isEmpty()) {
                // Then always diskless is disabled
                assertTrue(disklessConfigRecords.stream().allMatch(c -> c.value().equals("false")));
            }

            // Given the topic is registered
            ctx.replay(result.records());
            assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 0}).
                    setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00001DIRAAAA"),
                        Uuid.fromString("TESTBROKER00002DIRAAAA"),
                        Uuid.fromString("TESTBROKER00000DIRAAAA")
                    }).
                    setIsr(new int[] {1, 2, 0}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(0).build(),
                replicationControl.getPartition(
                    ((TopicRecord) result.records().get(0).message()).topicId(), 0));

            // When creating a topic with diskless enabled and already exists
            ControllerResult<CreateTopicsResponseData> result1 =
                replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
            CreateTopicsResponseData expectedResponse1 = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse1.topics().add(new CreatableTopicResult().setName("foo").
                setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code()).
                setErrorMessage("Topic 'foo' already exists."));
            assertEquals(expectedResponse1, result1.response());
        }

        @ParameterizedTest
        @CsvSource(value = {
            "true,true",
            "true,NULL",
            "false,true",
        }, nullValues = "NULL")
        public void testCreateDisklessTopic_noRacks(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given a request to create a kafka topic with diskless enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection creatableTopicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                creatableTopicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }
            request.topics().add(new CreatableTopic().setName("foo").
                setNumPartitions(-1).setReplicationFactor((short) -1)
                .setConfigs(creatableTopicConfigs));

            // When creating a topic without brokers available
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should fail with INVALID_REPLICATION_FACTOR
            // (standard Kafka behavior when no brokers can satisfy the requested RF)
            assertEquals(Errors.INVALID_REPLICATION_FACTOR.code(),
                result.response().topics().find("foo").errorCode());

            // Given brokers are registered
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result2 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should succeed with RF=3 (default.replication.factor),
            // regardless of fenced brokers
            CreateTopicsResponseData expectedResponse2 = new CreateTopicsResponseData();
            expectedResponse2.topics().add(new CreatableTopicResult().setName("foo").
                setNumPartitions(1).setReplicationFactor((short) 3).
                setErrorMessage(null).setErrorCode((short) 0).
                setTopicId(result2.response().topics().find("foo").topicId()));
            CreateTopicsResponseData response = result2.response();
            assertEquals(expectedResponse2, withoutConfigs(response));

            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result3 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should succeed with RF=3 (default.replication.factor)
            CreateTopicsResponseData expectedResponse3 = new CreateTopicsResponseData();
            expectedResponse3.topics().add(new CreatableTopicResult().setName("foo").
                setNumPartitions(1).setReplicationFactor((short) 3).
                setErrorMessage(null).setErrorCode((short) 0).
                setTopicId(result3.response().topics().find("foo").topicId()));
            assertEquals(expectedResponse3, withoutConfigs(result3.response()));
            final List<ConfigRecord> disklessConfigRecords = result3.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            assertEquals(1, disklessConfigRecords.size());
            // Then diskless is always enabled
            assertTrue(disklessConfigRecords.stream().allMatch(c -> c.value().equals("true")));

            // Given the topic is registered
            ctx.replay(result3.records());
            PartitionRegistration partition = replicationControl.getPartition(
                ((TopicRecord) result3.records().get(0).message()).topicId(), 0);
            assertEquals(3, partition.replicas.length, "RF should be 3 (default.replication.factor)");
            assertEquals(3, partition.isr.length, "All brokers are active so ISR should equal replicas");
            assertTrue(partition.leader >= 0, "Leader should be elected");

            // When creating a topic with diskless enabled and already exists
            ControllerResult<CreateTopicsResponseData> result4 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            CreateTopicsResponseData expectedResponse4 = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse4.topics().add(new CreatableTopicResult().setName("foo").
                setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code()).
                setErrorMessage("Topic 'foo' already exists."));
            assertEquals(expectedResponse4, result4.response());
        }

        @ParameterizedTest
        @CsvSource(value = {
            "true,true",
            "true,NULL",
            "false,true",
        }, nullValues = "NULL")
        public void testCreateDisklessTopic_withRacks(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given a request to create a kafka topic with diskless enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection creatableTopicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                creatableTopicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }
            request.topics().add(new CreatableTopic().setName("foo").
                setNumPartitions(-1).setReplicationFactor((short) -1)
                .setConfigs(creatableTopicConfigs));

            // When creating a topic without brokers available
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should fail with INVALID_REPLICATION_FACTOR
            // (standard Kafka behavior when no brokers can satisfy the requested RF)
            assertEquals(Errors.INVALID_REPLICATION_FACTOR.code(),
                result.response().topics().find("foo").errorCode());

            // Given brokers are registered
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result2 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should succeed, regardless of fenced brokers
            CreateTopicsResponseData expectedResponse2 = new CreateTopicsResponseData();
            expectedResponse2.topics().add(new CreatableTopicResult().setName("foo").
                setNumPartitions(1).setReplicationFactor((short) 3).
                setErrorMessage(null).setErrorCode((short) 0).
                setTopicId(result2.response().topics().find("foo").topicId()));
            CreateTopicsResponseData response = result2.response();
            assertEquals(expectedResponse2, withoutConfigs(response));

            // Given all brokers unfenced
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            // When creating a topic with diskless enabled
            ControllerResult<CreateTopicsResponseData> result3 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            // Then the topic creation should succeed, regardless of the RF
            CreateTopicsResponseData expectedResponse3 = new CreateTopicsResponseData();
            expectedResponse3.topics().add(new CreatableTopicResult().setName("foo").
                setNumPartitions(1).setReplicationFactor((short) 3).
                setErrorMessage(null).setErrorCode((short) 0).
                setTopicId(result3.response().topics().find("foo").topicId()));
            assertEquals(expectedResponse3, withoutConfigs(result3.response()));
            final List<ConfigRecord> disklessConfigRecords = result3.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            assertEquals(1, disklessConfigRecords.size());
            // Then diskless is always enabled
            assertTrue(disklessConfigRecords.stream().allMatch(c -> c.value().equals("true")));

            // Given the topic is registered
            ctx.replay(result3.records());
            assertEquals(
                new PartitionRegistration.Builder().setReplicas(new int[] {0, 1, 2}).
                    setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00000DIRAAAA"),
                        Uuid.fromString("TESTBROKER00001DIRAAAA"),
                        Uuid.fromString("TESTBROKER00002DIRAAAA"),
                    }).
                    setIsr(new int[] {0, 1, 2})
                    .setLeader(0)
                    .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                    .setLeaderEpoch(0)
                    .setPartitionEpoch(0)
                    .build(),
                replicationControl.getPartition(((TopicRecord) result3.records().get(0).message()).topicId(), 0));

            // When creating a topic with diskless enabled and already exists
            ControllerResult<CreateTopicsResponseData> result4 =
                replicationControl.createTopics(requestContext, request, Set.of("foo"));
            CreateTopicsResponseData expectedResponse4 = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse4.topics().add(new CreatableTopicResult().setName("foo").
                setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code()).
                setErrorMessage("Topic 'foo' already exists."));
            assertEquals(expectedResponse4, result4.response());
        }

        @ParameterizedTest
        @CsvSource({
            "1, -2, INVALID_REPLICATION_FACTOR",
            "1, 0, INVALID_REPLICATION_FACTOR",
            "-2, 1, INVALID_PARTITIONS",
            "0, 1, INVALID_PARTITIONS",
        })
        public void testCreateDisklessTopicWithInvalidInput(int numPartitions, short replicationFactor, String expectedError) {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);

            CreateTopicsRequestData.CreatableTopicConfigCollection disklessConfig =
                new CreateTopicsRequestData.CreatableTopicConfigCollection();
            disklessConfig.add(
                new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName("diskless.enable")
                    .setValue("true")
            );

            CreateTopicsRequestData request1 = new CreateTopicsRequestData();
            request1.topics().add(new CreatableTopic().setName("baz")
                .setNumPartitions(numPartitions).setReplicationFactor(replicationFactor)
                .setConfigs(disklessConfig));

            ControllerResult<CreateTopicsResponseData> result1 =
                replicationControl.createTopics(requestContext, request1, Set.of("baz"));
            assertEquals(Errors.valueOf(expectedError).code(), result1.response().topics().find("baz").errorCode());
            assertEquals(List.of(), result1.records());
        }

        @ParameterizedTest
        @CsvSource(value = {
            "true,false",
            "true,NULL"
            // This case is not valid because no internal topic should be explicitly created with diskless enabled.
            // Tested in testInvalidDisklessTopicCreationForInternalTopics
            // "false,true",
        }, nullValues = "NULL")
        public void testCreateInternalTopicWithDisklessEnabled(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            // Given a setup with diskless defined at the server level
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given an internal kafka topic with diskless enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection creatableTopicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                // If the diskless enable config is set, it should be added to the topic configs
                creatableTopicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }
            final String internalTopic = Topic.GROUP_METADATA_TOPIC_NAME;
            request.topics().add(new CreatableTopic().setName(internalTopic).
                setNumPartitions(-1).setReplicationFactor((short) -1)
                .setConfigs(creatableTopicConfigs));
            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            // When creating an internal topic with diskless enabled, disable it
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of(internalTopic));
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            // Then the topic creation should succeed with diskless disabled for internal topics
            expectedResponse.topics().add(
                new CreatableTopicResult()
                    .setName(internalTopic)
                    .setNumPartitions(1)
                    .setReplicationFactor((short) 3)
                    .setErrorMessage(null).setErrorCode((short) 0)
                    .setTopicId(result.response().topics().find(internalTopic).topicId()));
            assertEquals(expectedResponse, withoutConfigs(result.response()));
            assertTrue(result.response().topics().find(internalTopic)
                .configs()
                .stream()
                .noneMatch(c -> c.name().equals(DISKLESS_ENABLE_CONFIG)));
            final List<ConfigRecord> disklessConfigRecords = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(c -> c.name().equals(DISKLESS_ENABLE_CONFIG))
                .toList();
            // Then always diskless is disabled
            assertTrue(disklessConfigRecords.stream().allMatch(c -> c.value().equals("false")));
        }

        @Test
        public void testInvalidDisklessTopicCreationForInternalTopics() {
            // Given a setup with diskless defined at the server level
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given an internal kafka topic with diskless enabled
            final String internalTopic = Topic.GROUP_METADATA_TOPIC_NAME;
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection topicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            topicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(DISKLESS_ENABLE_CONFIG)
                .setValue("true"));
            request.topics().add(new CreatableTopic().setName(internalTopic).setConfigs(topicConfigs));
            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            // When creating an internal topic with diskless enabled, disable it
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of(internalTopic));
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse.topics().add(
                new CreatableTopicResult()
                    .setName(internalTopic)
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("System topics cannot be diskless topics."));
            assertEquals(expectedResponse, withoutConfigs(result.response()));
        }

        @ParameterizedTest
        @CsvSource(value = {
            "false,true",
            "true,NULL"
        }, nullValues = "NULL")
        public void testInvalidDisklessTopicCreationWithoutSystemEnabled(boolean logDisklessEnableServerConfig, String disklessEnableTopicConfig) {
            // Given a setup with diskless defined at the server level
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(false)
                .setDefaultDisklessEnable(logDisklessEnableServerConfig)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            // Given an internal kafka topic with diskless enabled
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreateTopicsRequestData.CreatableTopicConfigCollection topicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            if (disklessEnableTopicConfig != null) {
                // If the diskless enable config is set, it should be added to the topic configs
                topicConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                    .setName(DISKLESS_ENABLE_CONFIG)
                    .setValue(disklessEnableTopicConfig));
            }
            final String topicName = "foo";
            request.topics().add(new CreatableTopic().setName(topicName).setConfigs(topicConfigs));
            // Given all brokers unfenced
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            // When creating an internal topic with diskless enabled, disable it
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(requestContext, request, Set.of(topicName));
            CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
            // Then the topic creation should fail with TOPIC_ALREADY_EXISTS error
            expectedResponse.topics().add(
                new CreatableTopicResult()
                    .setName(topicName)
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Cannot create diskless topics when the diskless storage system is disabled. Please enable the diskless storage system to create diskless topics."));
            assertEquals(expectedResponse, withoutConfigs(result.response()));
        }

        @Test
        public void testReassignDisklessPartitions() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1);
            ctx.unfenceBrokers(0, 1);

            String topic = "foo";
            CreatableTopicResult createResult = ctx.createTestTopic(topic, new int[][] {new int[] {0}}, Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // No change in the replication factor.
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult1 =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName(topic).setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1)))))));
            assertEquals(new AlterPartitionReassignmentsResponseData().
                    setErrorMessage(null).setResponses(List.of(
                        new ReassignableTopicResponse().setName(topic).setPartitions(List.of(
                            new ReassignablePartitionResponse().setPartitionIndex(0).setErrorMessage(null))))),
                alterResult1.response());

            ctx.replay(alterResult1.records());

            // For diskless topics, reassignment completes immediately.
            // There should be no ongoing reassignment.
            assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(List.of(
                new ListPartitionReassignmentsTopics().setName(topic).
                    setPartitionIndexes(List.of(0))), Long.MAX_VALUE));

            // Verify the partition now has the new replica (reassignment completed immediately)
            PartitionRegistration partition = replication.getPartition(createResult.topicId(), 0);
            assertEquals(List.of(1), Replicas.toList(partition.replicas));
            // ISR must match replicas — diskless brokers are immediately in-sync via object storage
            assertEquals(List.of(1), Replicas.toList(partition.isr));

            // Try to increase the replication factor (should fail for diskless).
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult2 =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName(topic).setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0)
                                .setReplicas(List.of(0, 1)))))));
            assertEquals(new AlterPartitionReassignmentsResponseData()
                    .setErrorMessage(null).setResponses(List.of(
                        new ReassignableTopicResponse().setName(topic).setPartitions(List.of(
                            new ReassignablePartitionResponse().setPartitionIndex(0)
                                .setErrorCode(INVALID_REPLICATION_FACTOR.code())
                                .setErrorMessage("The replication factor is changed from 1 to 2"))))),
                alterResult2.response());
        }

        @Test
        public void testReassignDisklessPartitionsToFencedBrokerIncludesInIsr() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Create topic with RF=2 so we can reassign to a different pair without changing RF
            String topic = "foo";
            CreatableTopicResult createResult = ctx.createTestTopic(topic, new int[][] {new int[] {0, 1}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // Fence broker 2
            ctx.fenceBrokers(2);

            // Reassign to brokers 1 (active) and 2 (fenced) — same RF
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName(topic).setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1, 2)))))));
            assertEquals(new AlterPartitionReassignmentsResponseData()
                    .setErrorMessage(null).setResponses(List.of(
                        new ReassignableTopicResponse().setName(topic).setPartitions(List.of(
                            new ReassignablePartitionResponse().setPartitionIndex(0).setErrorMessage(null))))),
                alterResult.response());

            ctx.replay(alterResult.records());

            // Verify replicas include both, and ISR includes all replicas (diskless: data in object storage)
            PartitionRegistration partition = replication.getPartition(createResult.topicId(), 0);
            assertEquals(List.of(1, 2), Replicas.toList(partition.replicas));
            assertEquals(List.of(1, 2), Replicas.toList(partition.isr));
        }

        @Test
        public void testReassignDisklessPartitionsToAllFencedBrokersIsRejected() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1);
            ctx.unfenceBrokers(0, 1);

            String topic = "foo";
            ctx.createTestTopic(topic, new int[][] {new int[] {0}}, Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // Fence broker 1
            ctx.fenceBrokers(1);

            // Reassign to only the fenced broker — should be rejected
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName(topic).setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1)))))));
            assertEquals(new AlterPartitionReassignmentsResponseData()
                    .setErrorMessage(null).setResponses(List.of(
                        new ReassignableTopicResponse().setName(topic).setPartitions(List.of(
                            new ReassignablePartitionResponse().setPartitionIndex(0)
                                .setErrorCode(INVALID_REPLICA_ASSIGNMENT.code())
                                .setErrorMessage("None of the target replicas [1] are active."))))),
                alterResult.response());
        }

        @Test
        public void testPeriodicLeaderBalancingSkipsUnmanagedDisklessTopics() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(false)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1);
            ctx.unfenceBrokers(0, 1);

            // Create a diskless topic with RF=1 (unmanaged — no manual assignment allowed)
            String disklessTopic = "diskless-foo";
            ctx.createTestTopic(disklessTopic, 1, (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), NONE.code());

            // Create an imbalanced classic topic to prove the balancer is functional
            CreatableTopicResult classicResult = ctx.createTestTopic(
                "classic-foo", new int[][] {new int[] {0, 1}});
            // Reassign to [1, 0] — makes it imbalanced (preferred=1, leader=0)
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName("classic-foo").setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1, 0)))))));
            ctx.replay(alterResult.records());

            // Balancer produces records for the imbalanced classic topic but NOT for unmanaged diskless
            ControllerResult<Boolean> balanceResult = replication.maybeBalancePartitionLeaders();
            assertEquals(1, balanceResult.records().size(),
                "Balancer should produce exactly one record (classic topic only, not diskless)");

            // Verify only the classic topic was rebalanced
            ctx.replay(balanceResult.records());
            PartitionRegistration classicPartition = replication.getPartition(classicResult.topicId(), 0);
            assertEquals(1, classicPartition.leader,
                "Classic topic leader should move to preferred replica");
        }

        @Test
        public void testPeriodicLeaderBalancingRebalancesManagedDisklessTopics() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1);
            ctx.unfenceBrokers(0, 1);

            // Create a diskless topic with RF=2 (managed replicas)
            String disklessTopic = "diskless-foo";
            CreatableTopicResult createResult = ctx.createTestTopic(
                disklessTopic, new int[][] {new int[] {0, 1}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // Reassign to [1, 0] — this changes the preferred replica from 0 to 1
            // but the leader stays at 0 (diskless immediate reassignment doesn't force leader move)
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
                replication.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName(disklessTopic).setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1, 0)))))));
            ctx.replay(alterResult.records());

            // Verify partition has preferred replica 1 but leader 0 (imbalanced)
            PartitionRegistration partition = replication.getPartition(createResult.topicId(), 0);
            assertEquals(List.of(1, 0), Replicas.toList(partition.replicas));
            assertEquals(0, partition.leader);
            assertFalse(partition.hasPreferredLeader(),
                "Leader should not be the preferred replica after reassignment");

            // Periodic leader balancing SHOULD rebalance managed diskless topics
            ControllerResult<Boolean> balanceResult = replication.maybeBalancePartitionLeaders();
            assertFalse(balanceResult.records().isEmpty(),
                "Periodic leader balancing should rebalance managed diskless topics");

            // Replay balance records and verify leader moved to preferred replica
            ctx.replay(balanceResult.records());
            PartitionRegistration balanced = replication.getPartition(createResult.topicId(), 0);
            assertEquals(1, balanced.leader,
                "Leader should have moved to preferred replica after balancing");
            assertTrue(balanced.hasPreferredLeader(),
                "Partition should now have preferred leader after balancing");
        }

        @Test
        public void testManagedDisklessIsrExpandsOnBrokerUnfenced() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Create a diskless topic with RF=3
            String disklessTopic = "diskless-foo";
            CreatableTopicResult createResult = ctx.createTestTopic(
                disklessTopic, new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // Verify initial ISR = [0, 1, 2]
            PartitionRegistration partition = replication.getPartition(createResult.topicId(), 0);
            assertEquals(List.of(0, 1, 2), Replicas.toList(partition.isr));
            assertEquals(0, partition.leader);

            // Fence broker 0 (current leader) — ISR shrinks, leader moves
            ctx.fenceBrokers(0);
            partition = replication.getPartition(createResult.topicId(), 0);
            assertEquals(2, partition.isr.length, "ISR should shrink on fencing");
            assertNotEquals(0, partition.leader, "Leader should move away from fenced broker");

            // Unfence broker 0 — ISR should expand back to include all replicas
            ctx.unfenceBrokers(0, 1, 2);
            partition = replication.getPartition(createResult.topicId(), 0);
            assertEquals(3, partition.isr.length,
                "ISR should expand back to all replicas after unfencing for diskless managed topics");
            assertTrue(Replicas.contains(partition.isr, 0), "Broker 0 should be back in ISR");
            assertTrue(Replicas.contains(partition.isr, 1), "Broker 1 should be in ISR");
            assertTrue(Replicas.contains(partition.isr, 2), "Broker 2 should be in ISR");
        }

        @Test
        public void testAddPartitionsAutoPlacement() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Create a diskless topic with RF=2, 1 partition
            String topic = "foo";
            CreatableTopicResult createResult = ctx.createTestTopic(topic, new int[][] {new int[] {0, 1}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // Add 2 more partitions (auto-placement, no manual assignments)
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
            ControllerResult<List<CreatePartitionsTopicResult>> addResult =
                replication.createPartitions(requestContext, List.of(
                    new CreatePartitionsTopic().setName(topic).setCount(3).setAssignments(null)));
            assertEquals(NONE.code(), addResult.response().get(0).errorCode());
            ctx.replay(addResult.records());

            // Verify new partitions have RF=2 (inherited from existing partitions)
            for (int p = 0; p < 3; p++) {
                PartitionRegistration partition = replication.getPartition(createResult.topicId(), p);
                assertNotNull(partition, "Partition " + p + " should exist");
                assertEquals(2, partition.replicas.length,
                    "Partition " + p + " should have RF=2");
                assertTrue(partition.isr.length > 0,
                    "Partition " + p + " should have non-empty ISR");
            }
        }

        @Test
        public void testAddPartitionsManualAssignment() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Create a diskless topic with RF=2, 1 partition
            String topic = "foo";
            CreatableTopicResult createResult = ctx.createTestTopic(topic, new int[][] {new int[] {0, 1}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // Add 1 partition with manual assignment
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
            ControllerResult<List<CreatePartitionsTopicResult>> addResult =
                replication.createPartitions(requestContext, List.of(
                    new CreatePartitionsTopic().setName(topic).setCount(2).setAssignments(List.of(
                        new CreatePartitionsAssignment().setBrokerIds(List.of(1, 2))))));
            assertEquals(NONE.code(), addResult.response().get(0).errorCode());
            ctx.replay(addResult.records());

            // Verify new partition has the specified replicas
            PartitionRegistration partition = replication.getPartition(createResult.topicId(), 1);
            assertEquals(List.of(1, 2), Replicas.toList(partition.replicas));
            // ISR should include only active brokers (both are active)
            assertEquals(List.of(1, 2), Replicas.toList(partition.isr));
        }

        @Test
        public void testAddPartitionsIsrIncludesFencedBrokersForDiskless() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Create a diskless topic with RF=2, 1 partition
            String topic = "foo";
            CreatableTopicResult createResult = ctx.createTestTopic(topic, new int[][] {new int[] {0, 1}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // Fence broker 2
            ctx.fenceBrokers(2);

            // Add 1 partition with manual assignment including fenced broker
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
            ControllerResult<List<CreatePartitionsTopicResult>> addResult =
                replication.createPartitions(requestContext, List.of(
                    new CreatePartitionsTopic().setName(topic).setCount(2).setAssignments(List.of(
                        new CreatePartitionsAssignment().setBrokerIds(List.of(1, 2))))));
            assertEquals(NONE.code(), addResult.response().get(0).errorCode());
            ctx.replay(addResult.records());

            // For diskless topics, ISR includes all replicas regardless of fenced state
            PartitionRegistration partition = replication.getPartition(createResult.topicId(), 1);
            assertEquals(List.of(1, 2), Replicas.toList(partition.replicas));
            assertEquals(List.of(1, 2), Replicas.toList(partition.isr));
        }

        @Test
        public void testAddPartitionsRejectsAllFencedBrokersForDiskless() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            String topic = "foo";
            ctx.createTestTopic(topic, new int[][] {new int[] {0, 1}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);

            // Fence brokers 1 and 2
            ctx.fenceBrokers(1, 2);

            // Add partition with manual assignment where ALL target brokers are fenced
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
            ControllerResult<List<CreatePartitionsTopicResult>> addResult =
                replication.createPartitions(requestContext, List.of(
                    new CreatePartitionsTopic().setName(topic).setCount(2).setAssignments(List.of(
                        new CreatePartitionsAssignment().setBrokerIds(List.of(1, 2))))));
            assertEquals(INVALID_REPLICA_ASSIGNMENT.code(), addResult.response().get(0).errorCode(),
                "Should reject when all target brokers are fenced — no active leader possible");
        }

        @Test
        public void testCreateTopicManualAssignmentRejectsAllFencedBrokersForDiskless() {
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Fence brokers 1 and 2
            ctx.fenceBrokers(1, 2);

            // Create topic with manual assignment where all brokers are fenced — should be rejected
            ctx.createTestTopic("bar", new int[][] {new int[] {1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), INVALID_REPLICA_ASSIGNMENT.code());
        }

        @Test
        public void testNoLeaderElectionOnBrokerFenced_noRacks() {
            // With RF=1 (no racks), when the single replica is fenced, the leader goes offline
            // and no new leader can be elected since there are no other replicas.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                new int[][] {
                    new int[] {0}
                },
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerFenced(0, records);
            ctx.replay(records);

            PartitionRegistration partition = replication.getPartition(topicId, 0);
            assertNotNull(partition, "Partition should exist after leader fencing");
            assertArrayEquals(new int[]{0}, partition.isr, "ISR should remain unchanged as there is only one replica");
            assertEquals(-1, partition.leader, "Leader should be offline after fencing");
        }

        @Test
        public void testLeaderElectionOnBrokerFenced_withRacks() {
            // With RF=3 (racks), when the leader is fenced, a new leader should be elected
            // from the remaining replicas in other racks.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                1,
                (short) 3,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            PartitionRegistration partitionBefore = replication.getPartition(topicId, 0);
            int originalLeader = partitionBefore.leader;

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerFenced(originalLeader, records);
            ctx.replay(records);

            PartitionRegistration partition = replication.getPartition(topicId, 0);
            assertNotNull(partition, "Partition should exist after leader fencing");
            assertEquals(3, partition.replicas.length, "Replicas should remain unchanged (RF=3)");
            assertEquals(2, partition.isr.length, "ISR should shrink by 1 after fencing the leader");
            assertNotEquals(originalLeader, partition.leader, "Leader should change after fencing");
            assertTrue(partition.leader >= 0, "A new leader should be elected from remaining replicas");
        }

        @Test
        public void testNoReplicaChangeOnShutdown_noRacks() {
            // With RF=1 (no racks), when the single replica is shutdown, the leader goes offline
            // and no new leader can be elected since there are no other replicas.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                new int[][] {
                    new int[] {0}
                },
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerShutdown(0, true, records);
            ctx.replay(records);

            PartitionRegistration partition = replication.getPartition(topicId, 0);
            assertNotNull(partition, "Partition should exist after leader shutdown");
            assertArrayEquals(new int[]{0}, partition.isr, "ISR should remain unchanged as there is only one replica");
            assertEquals(-1, partition.leader, "Leader should be offline after shutdown");
        }

        @Test
        public void testReplicaChangeOnShutdown_withRacks() {
            // With RF=3 (racks), when the leader broker is shutdown, the ISR shrinks and a new leader
            // is elected from the remaining replicas in other racks.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                1,
                (short) 3,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            // Capture the actual leader before shutdown
            PartitionRegistration initialPartition = replication.getPartition(topicId, 0);
            int originalLeader = initialPartition.leader;

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerShutdown(originalLeader, true, records);
            ctx.replay(records);

            PartitionRegistration partition = replication.getPartition(topicId, 0);
            assertNotNull(partition, "Partition should exist after broker shutdown");
            assertEquals(3, partition.replicas.length, "Replicas should remain unchanged (RF=3)");
            assertEquals(2, partition.isr.length, "ISR should shrink by 1 after shutdown");
            assertTrue(partition.leader >= 0, "A new leader should be elected from remaining replicas");
            assertNotEquals(originalLeader, partition.leader, "Leader should change after shutdown");
        }

        @Test
        void testDisklessMarksLeaderOfflineOnUnregister_noRacks() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            final int numPartitions = 6;
            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                numPartitions,
                (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerUnregistered(0, 100, records);
            ctx.replay(records);

            // All partitions should remain present and keep the original replica/ISR,
            // only the leader should be marked offline if placed on the unregistered broker.
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                PartitionRegistration partition = replication.getPartition(topicId, partitionId);
                assertNotNull(partition, "Partition " + partitionId + " should exist after broker unregistration");
                assertEquals(1, partition.replicas.length, "Replicas [" + Arrays.toString(partition.replicas) + "] should stay unchanged for partition " + partitionId);
                assertEquals(1, partition.isr.length, "ISR [" + Arrays.toString(partition.isr) + "] should stay unchanged for partition " + partitionId);
                if (partition.preferredReplica() == 0) {
                    assertEquals(-1, partition.leader, "Leader should be offline for partition " + partitionId);
                } else {
                    assertTrue(partition.leader > 0, "Leader should be online for partition " + partitionId);
                }
            }
            // Sticking to keep partitions offline, as availability is managed by the Diskless metadata transformation
            // with a fallback to "any node available"; not the KRaft registered metadata.
            // Replicas will be reported as offline, so operators are aware of the underprovisioning, and can act on it.
            // If they need to move the replicas, they can do that using regular tooling.
        }

        @Test
        void testDisklessMarksLeaderOfflineOnUnregister_withRacks() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            final int numPartitions = 6;
            CreatableTopicResult createResult = ctx.createTestTopic(
                "foo",
                numPartitions,
                (short) 3,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
            final Uuid topicId = createResult.topicId();

            List<ApiMessageAndVersion> records = new ArrayList<>();
            replication.handleBrokerUnregistered(0, 100, records);
            ctx.replay(records);

            // All partitions should remain present with original replicas.
            // ISR should shrink and leaders should move to other brokers.
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                PartitionRegistration partition = replication.getPartition(topicId, partitionId);
                assertNotNull(partition, "Partition " + partitionId + " should exist after broker unregistration");
                assertEquals(3, partition.replicas.length, "Replicas should stay unchanged for partition " + partitionId);
                assertEquals(2, partition.isr.length, "ISR should shrink to 2 for partition " + partitionId);
                assertTrue(partition.leader > 0, "Leader should remain online for partition " + partitionId);
            }
        }

        @Test
        void testManualReplicaAssignmentsShouldBeAllowed() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Expectation: providing manual replica assignments for a diskless topic with managed-replicas should be allowed.
            ctx.createTestTopic(
                "foo",
                new int[][] {new int[] {0, 1}, new int[] {1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "true"),
                NONE.code()
            );
        }

        @Test
        void testCreateDisklessTopicWithExplicitRF() {
            // Verify that explicit RF=2 is honored (not overridden to rack count or rejected).
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokersWithRacks(0, "a", 1, "b", 2, "c");
            ctx.unfenceBrokers(0, 1, 2);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            request.topics().add(new CreatableTopic().setName("foo")
                .setNumPartitions(2).setReplicationFactor((short) 2)
                .setConfigs(new CreateTopicsRequestData.CreatableTopicConfigCollection(List.of(
                    new CreateTopicsRequestData.CreatableTopicConfig()
                        .setName(DISKLESS_ENABLE_CONFIG)
                        .setValue("true")
                ).iterator())));

            ControllerResult<CreateTopicsResponseData> result =
                replication.createTopics(requestContext, request, Set.of("foo"));
            assertEquals(NONE.code(), result.response().topics().find("foo").errorCode());
            assertEquals(2, result.response().topics().find("foo").replicationFactor());
            assertEquals(2, result.response().topics().find("foo").numPartitions());

            ctx.replay(result.records());
            PartitionRegistration partition = replication.getPartition(
                ((TopicRecord) result.records().get(0).message()).topicId(), 0);
            assertEquals(2, partition.replicas.length, "RF=2 should be honored");
            assertEquals(2, partition.isr.length, "All replicas should be in ISR");
        }

        @Test
        void testCreateDisklessTopicWithRFExceedingBrokerCount() {
            // Verify that RF > broker count fails with standard Kafka error.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            request.topics().add(new CreatableTopic().setName("foo")
                .setNumPartitions(1).setReplicationFactor((short) 5)
                .setConfigs(new CreateTopicsRequestData.CreatableTopicConfigCollection(List.of(
                    new CreateTopicsRequestData.CreatableTopicConfig()
                        .setName(DISKLESS_ENABLE_CONFIG)
                        .setValue("true")
                ).iterator())));

            ControllerResult<CreateTopicsResponseData> result =
                replication.createTopics(requestContext, request, Set.of("foo"));
            assertEquals(Errors.INVALID_REPLICATION_FACTOR.code(),
                result.response().topics().find("foo").errorCode());
        }

        @Test
        void testCreateDisklessTopicWithRFGreaterThanOneRejectedWhenManagedDisabled() {
            // When managed replicas is disabled, RF > 1 should be rejected.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(false)
                .build();
            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            request.topics().add(new CreatableTopic().setName("foo")
                .setNumPartitions(1).setReplicationFactor((short) 2)
                .setConfigs(new CreateTopicsRequestData.CreatableTopicConfigCollection(List.of(
                    new CreateTopicsRequestData.CreatableTopicConfig()
                        .setName(DISKLESS_ENABLE_CONFIG)
                        .setValue("true")
                ).iterator())));

            ControllerResult<CreateTopicsResponseData> result =
                replication.createTopics(requestContext, request, Set.of("foo"));
            assertEquals(Errors.INVALID_REPLICATION_FACTOR.code(),
                result.response().topics().find("foo").errorCode());
        }

        @Test
        public void testUnfenceExpandsIsrAndClearsElr() {
            // Regression test: expandIsrForDisklessManagedPartitions must reconcile ELR so that
            // a broker added back to ISR on unfence is removed from ELR (ISR ∩ ELR = ∅, KIP-966).
            MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .setIsElrEnabled(true)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .build();

            ReplicationControlManager replication = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            // Create a diskless topic with RF=3 and minISR=3 so any fencing drives ISR below minISR.
            CreatableTopicResult createResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}}, Map.of(DISKLESS_ENABLE_CONFIG, "true"), (short) 0);
            ctx.alterTopicConfig("foo", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3");
            Uuid topicId = createResult.topicId();

            // Fence broker 2: ISR shrinks to {0, 1}, ELR gets {2} (ISR < minISR=3).
            ctx.fenceBrokers(2);
            PartitionRegistration afterFence = replication.getPartition(topicId, 0);
            assertFalse(Replicas.contains(afterFence.isr, 2), "broker 2 must be out of ISR after fencing");
            assertTrue(Replicas.contains(afterFence.elr, 2), "broker 2 must be in ELR after fencing");

            // Unfence broker 2: expandIsrForDisklessManagedPartitions fires.
            ctx.unfenceBrokers(2);
            PartitionRegistration afterUnfence = replication.getPartition(topicId, 0);

            // ISR must contain broker 2 again.
            assertTrue(Replicas.contains(afterUnfence.isr, 2), "broker 2 must be back in ISR after unfencing");
            // ELR must NOT contain broker 2 — ISR ∩ ELR = ∅ invariant (KIP-966).
            assertFalse(Replicas.contains(afterUnfence.elr, 2), "broker 2 must not be in ELR after ISR expansion");
        }
    }

    @Nested
    class DisklessRemoteStorageConsolidationTests {

        @Test
        void testAutoEnableRemoteStorageOnDisklessTopicCreation() {
            // When consolidation is enabled and a diskless topic is created with explicit
            // diskless.enable=true but no remote.storage.enable, auto-persist it.
            final ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            final CreateTopicsRequestData request = new CreateTopicsRequestData();
            final CreateTopicsRequestData.CreatableTopicConfigCollection configs =
                new CreateTopicsRequestData.CreatableTopicConfigCollection();
            configs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(DISKLESS_ENABLE_CONFIG)
                .setValue("true"));
            request.topics().add(new CreatableTopic()
                .setName("foo")
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1)
                .setConfigs(configs));

            final ControllerResult<CreateTopicsResponseData> result = ctx.replicationControl.createTopics(
                anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Set.of("foo"));
            assertEquals(NONE.code(), result.response().topics().find("foo").errorCode());
            assertTrue(result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .anyMatch(r -> r.name().equals(REMOTE_LOG_STORAGE_ENABLE_CONFIG) && r.value().equals("true")),
                "ConfigRecord for remote.storage.enable=true should be persisted");
        }

        @Test
        void testAutoEnableRemoteStorageViaDefaultDisklessEnable() {
            // When defaultDisklessEnable=true and topic is created with no explicit configs,
            // remote.storage.enable=true is still auto-persisted.
            final ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(true)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            final CreateTopicsRequestData request = new CreateTopicsRequestData();
            request.topics().add(new CreatableTopic()
                .setName("foo")
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1)
                .setConfigs(new CreateTopicsRequestData.CreatableTopicConfigCollection()));

            final ControllerResult<CreateTopicsResponseData> result = ctx.replicationControl.createTopics(
                anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Set.of("foo"));
            assertEquals(NONE.code(), result.response().topics().find("foo").errorCode());
            List<ConfigRecord> configRecords = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .toList();
            assertTrue(configRecords.stream()
                .anyMatch(r -> r.name().equals(DISKLESS_ENABLE_CONFIG) && r.value().equals("true")),
                "ConfigRecord for diskless.enable=true should be persisted via defaultDisklessEnable");
            assertTrue(configRecords.stream()
                .anyMatch(r -> r.name().equals(REMOTE_LOG_STORAGE_ENABLE_CONFIG) && r.value().equals("true")),
                "ConfigRecord for remote.storage.enable=true should be persisted via defaultDisklessEnable");
        }

        @Test
        void testExplicitRemoteStorageEnableDoesNotDuplicate() {
            // When the request already includes remote.storage.enable=true,
            // no duplicate ConfigRecord is produced.
            final ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            final CreateTopicsRequestData request = new CreateTopicsRequestData();
            final CreateTopicsRequestData.CreatableTopicConfigCollection configs =
                new CreateTopicsRequestData.CreatableTopicConfigCollection();
            configs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(DISKLESS_ENABLE_CONFIG)
                .setValue("true"));
            configs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(REMOTE_LOG_STORAGE_ENABLE_CONFIG)
                .setValue("true"));
            request.topics().add(new CreatableTopic()
                .setName("foo")
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1)
                .setConfigs(configs));

            final ControllerResult<CreateTopicsResponseData> result = ctx.replicationControl.createTopics(
                anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Set.of("foo"));
            assertEquals(NONE.code(), result.response().topics().find("foo").errorCode());
            long remoteStorageConfigCount = result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .filter(r -> r.name().equals(REMOTE_LOG_STORAGE_ENABLE_CONFIG))
                .count();
            assertEquals(1, remoteStorageConfigCount,
                "Only one ConfigRecord for remote.storage.enable should exist (no duplicate)");
        }

        @Test
        void testSystemTopicExcludedFromAutoEnable() {
            // System topics are never diskless, so remote.storage.enable should not be auto-added
            // even when defaultDisklessEnable=true and consolidation is enabled.
            final ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDefaultDisklessEnable(true)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            final CreateTopicsRequestData request = new CreateTopicsRequestData();
            request.topics().add(new CreatableTopic()
                .setName(Topic.GROUP_METADATA_TOPIC_NAME)
                .setNumPartitions(1)
                .setReplicationFactor((short) 1)
                .setConfigs(new CreateTopicsRequestData.CreatableTopicConfigCollection()));

            final ControllerResult<CreateTopicsResponseData> result = ctx.replicationControl.createTopics(
                anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Set.of(Topic.GROUP_METADATA_TOPIC_NAME));
            assertEquals(NONE.code(), result.response().topics().find(Topic.GROUP_METADATA_TOPIC_NAME).errorCode());
            assertTrue(result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .noneMatch(r -> r.name().equals(REMOTE_LOG_STORAGE_ENABLE_CONFIG)),
                "System topics should not get remote.storage.enable ConfigRecord");
        }

        @Test
        void testConsolidationDisabledDoesNotAutoEnable() {
            // When consolidation is disabled, remote.storage.enable is not auto-added
            // even for diskless topics.
            final ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(false)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            final CreateTopicsRequestData request = new CreateTopicsRequestData();
            final CreateTopicsRequestData.CreatableTopicConfigCollection configs =
                new CreateTopicsRequestData.CreatableTopicConfigCollection();
            configs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(DISKLESS_ENABLE_CONFIG)
                .setValue("true"));
            request.topics().add(new CreatableTopic()
                .setName("foo")
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1)
                .setConfigs(configs));

            final ControllerResult<CreateTopicsResponseData> result = ctx.replicationControl.createTopics(
                anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Set.of("foo"));
            assertEquals(NONE.code(), result.response().topics().find("foo").errorCode());
            assertTrue(result.records().stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .noneMatch(r -> r.name().equals(REMOTE_LOG_STORAGE_ENABLE_CONFIG)),
                "remote.storage.enable should not be auto-added when consolidation is disabled");
        }

        @Test
        void testExplicitRemoteStorageFalseRejected() {
            // Creating a diskless topic with remote.storage.enable=false when consolidation
            // is enabled must fail — the controller rejects the invalid combination.
            final ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            final CreateTopicsRequestData request = new CreateTopicsRequestData();
            final CreateTopicsRequestData.CreatableTopicConfigCollection configs =
                new CreateTopicsRequestData.CreatableTopicConfigCollection();
            configs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(DISKLESS_ENABLE_CONFIG)
                .setValue("true"));
            configs.add(new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(REMOTE_LOG_STORAGE_ENABLE_CONFIG)
                .setValue("false"));
            request.topics().add(new CreatableTopic()
                .setName("foo")
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1)
                .setConfigs(configs));

            final ControllerResult<CreateTopicsResponseData> result = ctx.replicationControl.createTopics(
                anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Set.of("foo"));
            assertEquals(Errors.INVALID_CONFIG.code(), result.response().topics().find("foo").errorCode(),
                "Diskless topic with remote.storage.enable=false should be rejected");
        }

        // ---- classic-to-diskless switch: auto-enable remote-storage atomically ----

        private ReplicationControlTestContext.Builder consolidationSwitchCtxBuilder() {
            return new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessAllowFromClassicEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(true);
        }

        private Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> setDisklessTrue(String topic) {
            return Map.of(new ConfigResource(ConfigResource.Type.TOPIC, topic),
                Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
        }

        @Test
        void testSwitchInjectsRemoteStorageEnableForClassicUntiered() {
            // A classic-untiered topic switching to diskless gets remote.storage.enable=true injected
            // into the incremental AlterConfigs request, so it is validated and persisted in the same
            // atomic batch as diskless.enable (mirroring topic creation).
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(setDisklessTrue("foo"));

            Entry<AlterConfigOp.OpType, String> rs = augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG);
            assertNotNull(rs, "remote.storage.enable should be injected on the switch");
            assertEquals(AlterConfigOp.OpType.SET, rs.getKey());
            assertEquals("true", rs.getValue());
            // diskless.enable is preserved untouched
            assertEquals("true", augmented.get(resource).get(DISKLESS_ENABLE_CONFIG).getValue());
        }

        @Test
        void testSwitchDoesNotDuplicateRemoteStorageForClassicTiered() {
            // A classic-tiered topic (remote.storage.enable already true) switching to diskless must NOT
            // get a second remote.storage.enable change injected — RS is already on.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), NONE.code());

            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> input = setDisklessTrue("foo");
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(input);

            // No augmentation: returns the original map unchanged.
            assertSame(input, augmented);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            assertFalse(augmented.get(resource).containsKey(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
        }

        @Test
        void testSwitchPreservesExplicitRemoteStorageInRequest() {
            // When the switch request already carries remote.storage.enable explicitly, it is left as-is
            // (validation rejects an explicit false; an explicit true needs no help).
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> input = Map.of(
                resource, Map.of(
                    DISKLESS_ENABLE_CONFIG, new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"),
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "false")));

            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(input);
            assertSame(input, augmented);
            assertEquals("false", augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG).getValue());
        }

        @Test
        void testSwitchInjectsRemoteStorageEnableWhenRequestDeletesIt() {
            // A switch request that DELETEs remote.storage.enable would leave the topic untiered
            // diskless. The DELETE is not treated as "provided": remote.storage.enable=true is
            // injected (overwriting the DELETE) so the switch stays atomic.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> input = Map.of(
                resource, Map.of(
                    DISKLESS_ENABLE_CONFIG, new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"),
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.DELETE, null)));

            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(input);

            Entry<AlterConfigOp.OpType, String> rs = augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG);
            assertEquals(AlterConfigOp.OpType.SET, rs.getKey(), "the DELETE should be overwritten with a SET");
            assertEquals("true", rs.getValue());
        }

        @Test
        void testSwitchInjectsRemoteStorageEnableWhenRequestSetsNullValue() {
            // A valueless SET (null value) on remote.storage.enable strips the override just like a
            // DELETE, so it is not treated as "provided": remote.storage.enable=true is injected
            // (overwriting the null SET) to keep the switch atomic.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> input = Map.of(
                resource, Map.of(
                    DISKLESS_ENABLE_CONFIG, new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"),
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, null)));

            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(input);

            Entry<AlterConfigOp.OpType, String> rs = augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG);
            assertEquals(AlterConfigOp.OpType.SET, rs.getKey());
            assertEquals("true", rs.getValue(), "a null-valued SET should be overwritten with SET true");
        }

        @Test
        void testSwitchOverridesRemoteStorageDeleteForClassicTiered() {
            // A classic-tiered topic (remote.storage.enable already true) whose switch request DELETEs
            // remote.storage.enable must still get true injected: the DELETE would otherwise strip the
            // existing override and leave the topic untiered diskless.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), NONE.code());

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> input = Map.of(
                resource, Map.of(
                    DISKLESS_ENABLE_CONFIG, new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"),
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.DELETE, null)));

            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(input);

            Entry<AlterConfigOp.OpType, String> rs = augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG);
            assertEquals(AlterConfigOp.OpType.SET, rs.getKey(), "the DELETE should be overwritten with a SET");
            assertEquals("true", rs.getValue());
        }

        @Test
        void testSwitchInjectsRemoteStorageEnableWhenConsolidationDisabled() {
            // The injection is gated on the switch flag, not consolidation: even with consolidation off,
            // a classic-untiered switch gets remote.storage.enable=true, so a switched topic is never
            // untiered diskless. It consolidates once consolidation is enabled (which requires the switch
            // flag), without any remote-storage re-enable step.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessAllowFromClassicEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(false)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(setDisklessTrue("foo"));

            Entry<AlterConfigOp.OpType, String> rs = augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG);
            assertNotNull(rs, "remote.storage.enable should be injected on the switch even without consolidation");
            assertEquals(AlterConfigOp.OpType.SET, rs.getKey());
            assertEquals("true", rs.getValue());
        }

        @Test
        void testSwitchNoInjectionWhenAllowFromClassicDisabled() {
            // The switch flag gates the injection: with allow-from-classic off there is no switch to
            // augment (the switch itself is rejected elsewhere), so the request is returned unchanged.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, false)
                .setDisklessStorageSystemEnabled(true)
                .setDisklessManagedReplicasEnabled(true)
                .setDisklessRemoteStorageConsolidationEnabled(false)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> input = setDisklessTrue("foo");
            assertSame(input, ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(input));
        }

        @Test
        void testSwitchNoInjectionForAlreadyDisklessTopic() {
            // A topic that is already diskless is not "switching"; no RS change is injected.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", 1, (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), NONE.code());

            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> input = setDisklessTrue("foo");
            assertSame(input, ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(input));
        }

        @Test
        void testLegacyAlterConfigsSwitchInjectsRemoteStorageEnableForClassicUntiered() {
            // Legacy AlterConfigs API (full config map) on a classic-untiered topic switching to diskless
            // also injects remote.storage.enable=true.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, String>> input = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
            Map<ConfigResource, Map<String, String>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForLegacyAlterConfigs(input);

            assertEquals("true", augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG),
                "remote.storage.enable should be injected on the legacy AlterConfigs switch");
            assertEquals("true", augmented.get(resource).get(DISKLESS_ENABLE_CONFIG));
        }

        @Test
        void testLegacyAlterConfigsSwitchRepinsRemoteStorageForClassicTiered() {
            // A classic-tiered topic (remote.storage.enable already true) switched via legacy AlterConfigs
            // that omits remote.storage.enable: the full-map replace would implicitly delete the override
            // and leave the topic untiered diskless. Injection re-pins remote.storage.enable=true. The
            // injected value equals the stored value, so no config record results; it only keeps the key
            // present so it is not implicitly deleted.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), NONE.code());

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, String>> input = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
            Map<ConfigResource, Map<String, String>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForLegacyAlterConfigs(input);

            assertEquals("true", augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG),
                "remote.storage.enable must be re-pinned so the full-map replace does not delete it");
            assertEquals("true", augmented.get(resource).get(DISKLESS_ENABLE_CONFIG));
        }

        @Test
        void testLegacyAlterConfigsSwitchInjectsRemoteStorageEnableWhenValueIsNull() {
            // A legacy switch request carrying remote.storage.enable with a null value would strip the
            // override on the full-map replace, so it is injected as true (not treated as "provided").
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<String, String> configs = new HashMap<>();
            configs.put(DISKLESS_ENABLE_CONFIG, "true");
            configs.put(REMOTE_LOG_STORAGE_ENABLE_CONFIG, null);
            Map<ConfigResource, Map<String, String>> input = Map.of(resource, configs);

            Map<ConfigResource, Map<String, String>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForLegacyAlterConfigs(input);

            assertEquals("true", augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG),
                "a null remote.storage.enable value must be overwritten with true");
        }

        @Test
        void testLegacyAlterConfigsSwitchPreservesExplicitRemoteStorageFalse() {
            // An explicit remote.storage.enable=false in a legacy switch request is left as-is (not
            // overwritten) so validation rejects the contradictory switch (diskless with remote off).
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, String>> input = Map.of(
                resource, Map.of(
                    DISKLESS_ENABLE_CONFIG, "true",
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"));
            Map<ConfigResource, Map<String, String>> augmented =
                ctx.replicationControl.maybeAddRemoteStorageEnableForLegacyAlterConfigs(input);

            assertSame(input, augmented);
            assertEquals("false", augmented.get(resource).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
        }

        @Test
        void testLegacyAlterConfigsImplicitDisklessDeletionRejectedNoHalfState() {
            // A legacy AlterConfigs that omits diskless.enable on a classic topic (cluster default
            // diskless) would implicitly delete the classic pin. That implicit switch is rejected up
            // front, matching the incremental DELETE guard. Even though remote-storage injection runs
            // before validation, the whole request is rejected, so no config records and no switch-pending
            // markers are produced — no half-applied state.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder()
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false"), NONE.code());

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            // Legacy request omits diskless.enable (only carries an unrelated config).
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(
                resource, Map.of(SEGMENT_BYTES_CONFIG, "1048576"));

            // Drive the same wiring QuorumController.legacyAlterConfigs uses.
            Map<ConfigResource, Map<String, String>> effective =
                ctx.replicationControl.maybeAddRemoteStorageEnableForLegacyAlterConfigs(newConfigs);
            ControllerResult<Map<ConfigResource, ApiError>> configResult =
                ctx.configurationControl.legacyAlterConfigs(effective, false,
                    r -> ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(r, effective));

            assertEquals(Errors.INVALID_CONFIG, configResult.response().get(resource).error(),
                "Implicit diskless.enable deletion via legacy AlterConfigs must be rejected");
            assertTrue(configResult.response().get(resource).message().contains("not allowed to delete"),
                "Expected delete-rejection message, got: " + configResult.response().get(resource).message());
            assertTrue(configResult.records().isEmpty(),
                "Rejected legacy AlterConfigs must not emit config records");

            List<ApiMessageAndVersion> migrationRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStartedForLegacyAlterConfigs(
                    effective, configResult.response());
            assertTrue(migrationRecords.isEmpty(),
                "Rejected implicit switch must not emit switch-pending markers");
        }

        @Test
        void testSwitchCommitsRemoteStorageDisklessAndPendingInOneBatch() {
            // The load-bearing invariant: on a classic-untiered switch, the augmented
            // remote.storage.enable=true ConfigRecord, the diskless.enable=true ConfigRecord,
            // and the per-partition PENDING PartitionChangeRecords must all land in ONE atomic batch.
            // This drives the same wiring QuorumController.incrementalAlterConfigs uses
            // (augment, validate, then merge with the marker records),
            // and asserts all three record kinds co-commit.
            ReplicationControlTestContext ctx = consolidationSwitchCtxBuilder().build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult foo = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}, new int[] {1, 2, 0}});
            Uuid topicId = foo.topicId();

            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> requested = setDisklessTrue("foo");

            // 1. Augment before validating (QuorumController step 1).
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> effective =
                ctx.replicationControl.maybeAddRemoteStorageEnableForSwitch(requested);

            // 2. Validate + generate config records (QuorumController step 2).
            ControllerResult<Map<ConfigResource, ApiError>> configResult =
                ctx.configurationControl.incrementalAlterConfigs(effective, false,
                    resource -> ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, effective));
            assertEquals(ApiError.NONE,
                configResult.response().get(new ConfigResource(ConfigResource.Type.TOPIC, "foo")));

            // 3. Generate the per-partition switch-pending markers (QuorumController step 3).
            List<ApiMessageAndVersion> migrationRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStarted(effective, configResult.response());

            // 4. Merge into the one batch QuorumController commits via ControllerResult.atomicOf.
            List<ApiMessageAndVersion> batch = new ArrayList<>();
            batch.addAll(configResult.records());
            batch.addAll(migrationRecords);

            List<ConfigRecord> configRecords = batch.stream()
                .filter(m -> m.message() instanceof ConfigRecord)
                .map(m -> (ConfigRecord) m.message())
                .toList();
            assertTrue(configRecords.stream()
                    .anyMatch(r -> r.name().equals(DISKLESS_ENABLE_CONFIG) && r.value().equals("true")),
                "diskless.enable=true ConfigRecord must be in the batch");
            assertTrue(configRecords.stream()
                    .anyMatch(r -> r.name().equals(REMOTE_LOG_STORAGE_ENABLE_CONFIG) && r.value().equals("true")),
                "remote.storage.enable=true ConfigRecord must be in the batch");

            List<PartitionChangeRecord> pendingRecords = batch.stream()
                .filter(m -> m.message() instanceof PartitionChangeRecord)
                .map(m -> (PartitionChangeRecord) m.message())
                .filter(r -> r.topicId().equals(topicId))
                .filter(r -> InitDisklessLogFields.decodeClassicToDisklessStartOffset(r.unknownTaggedFields())
                    == PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
                .toList();
            assertEquals(2, pendingRecords.size(),
                "both partitions must get a PENDING PartitionChangeRecord in the same batch");

            // Replaying the whole batch leaves a coherent diskless topic (no half-applied state).
            ctx.replay(batch);
            assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING,
                ctx.replicationControl.getPartition(topicId, 0).classicToDisklessStartOffset);
        }
    }

    @Nested
    class InitDisklessLogTests {
        private void markSwitchPending(ReplicationControlTestContext ctx, Uuid topicId, int partitionId) {
            PartitionChangeRecord switchPendingRecord = new PartitionChangeRecord()
                .setTopicId(topicId)
                .setPartitionId(partitionId);
            switchPendingRecord.unknownTaggedFields().add(
                InitDisklessLogFields.encodeClassicToDisklessStartOffset(
                    PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING));
            ctx.replay(List.of(new ApiMessageAndVersion(switchPendingRecord, (short) 0)));
        }

        private InitDisklessLogRequestData singlePartitionRequest(
            int brokerId,
            long brokerEpoch,
            Uuid topicId,
            int partitionId,
            long classicToDisklessStartOffset,
            int leaderEpoch,
            List<InitDisklessLogRequestData.ProducerState> producerStates
        ) {
            InitDisklessLogRequestData request = new InitDisklessLogRequestData()
                .setBrokerId(brokerId)
                .setBrokerEpoch(brokerEpoch);
            InitDisklessLogRequestData.TopicData topicData = new InitDisklessLogRequestData.TopicData()
                .setTopicId(topicId);
            InitDisklessLogRequestData.PartitionData partitionData = new InitDisklessLogRequestData.PartitionData()
                .setPartitionId(partitionId)
                .setDisklessStartOffset(classicToDisklessStartOffset)
                .setLeaderEpoch(leaderEpoch);
            partitionData.producerStates().addAll(producerStates);
            topicData.partitions().add(partitionData);
            request.topics().add(topicData);
            return request;
        }

        @Test
        public void testInitDisklessLogSuccess() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            markSwitchPending(ctx, topicId, 0);
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);
            assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING, partition.classicToDisklessStartOffset);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0,
                defaultBrokerEpoch(0),
                topicId,
                0,
                100L,
                partition.leaderEpoch,
                List.of(new InitDisklessLogRequestData.ProducerState()
                    .setProducerId(42L)
                    .setProducerEpoch((short) 1)
                    .setBaseSequence(0)
                    .setLastSequence(5)
                    .setAssignedOffset(200L)
                    .setBatchMaxTimestamp(1000L))
            );

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(1, result.records().size());
            assertInstanceOf(PartitionChangeRecord.class, result.records().get(0).message());

            PartitionChangeRecord record = (PartitionChangeRecord) result.records().get(0).message();
            assertEquals(topicId, record.topicId());
            assertEquals(0, record.partitionId());
            assertEquals(100L, InitDisklessLogFields.decodeClassicToDisklessStartOffset(record.unknownTaggedFields()));
            // The change record captures the partition's current leader epoch as the diskless leader epoch.
            assertEquals(partition.leaderEpoch,
                InitDisklessLogFields.decodeDisklessLeaderEpoch(record.unknownTaggedFields()));

            List<InitDisklessLogFields.ProducerStateEntry> producerStates =
                InitDisklessLogFields.decodeProducerStates(record.unknownTaggedFields());
            assertEquals(1, producerStates.size());
            assertEquals(42L, producerStates.get(0).producerId());
            assertEquals((short) 1, producerStates.get(0).producerEpoch());
            assertEquals(0, producerStates.get(0).baseSequence());
            assertEquals(5, producerStates.get(0).lastSequence());
            assertEquals(200L, producerStates.get(0).assignedOffset());
            assertEquals(1000L, producerStates.get(0).batchMaxTimestamp());

            InitDisklessLogResponseData response = result.response();
            assertEquals(1, response.topics().size());
            assertEquals(topicId, response.topics().get(0).topicId());
            assertEquals(1, response.topics().get(0).partitions().size());
            assertEquals(0, response.topics().get(0).partitions().get(0).partitionId());
            assertEquals(NONE.code(), response.topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogReplayUpdatesDisklessFields() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            markSwitchPending(ctx, topicId, 0);
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 42L, partition.leaderEpoch, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);
            ctx.replay(result.records());

            PartitionRegistration updatedPartition = replicationControl.getPartition(topicId, 0);
            assertEquals(42L, updatedPartition.classicToDisklessStartOffset);
            assertTrue(updatedPartition.disklessProducerStates.isEmpty());
            assertEquals(partition.leaderEpoch, updatedPartition.disklessLeaderEpoch);
        }

        @Test
        public void testInitDisklessLogUnknownTopicId() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);

            Uuid unknownTopicId = Uuid.randomUuid();
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), unknownTopicId, 0, 100L, 0, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(0, result.records().size());
            assertEquals(UNKNOWN_TOPIC_ID.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogUnknownPartition() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 99, 100L, 0, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(0, result.records().size());
            assertEquals(UNKNOWN_TOPIC_OR_PARTITION.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogStaleBrokerEpoch() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = new InitDisklessLogRequestData()
                .setBrokerId(0)
                .setBrokerEpoch(defaultBrokerEpoch(0) - 1);

            assertThrows(StaleBrokerEpochException.class,
                () -> replicationControl.initDisklessLog(requestContext, request));
        }

        @Test
        public void testInitDisklessLogHigherLeaderEpoch() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 100L, partition.leaderEpoch + 10, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(0, result.records().size());
            assertEquals(NOT_CONTROLLER.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogLowerLeaderEpoch() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);
            assertTrue(partition.leaderEpoch > 0 || partition.leaderEpoch == 0);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 100L, partition.leaderEpoch - 1, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(0, result.records().size());
            assertEquals(FENCED_LEADER_EPOCH.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogNotLeader() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);
            int notLeaderId = (partition.leader == 0) ? 1 : 0;

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                notLeaderId, defaultBrokerEpoch(notLeaderId), topicId, 0, 100L, partition.leaderEpoch, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(0, result.records().size());
            assertEquals(INVALID_REQUEST.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogNegativeStartOffset() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, -1L, partition.leaderEpoch, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(0, result.records().size());
            assertEquals(INVALID_REQUEST.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogAlreadyInitializedPartitionRejected() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            markSwitchPending(ctx, topicId, 0);
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData firstRequest = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 100L, partition.leaderEpoch, List.of());

            ControllerResult<InitDisklessLogResponseData> firstResult =
                replicationControl.initDisklessLog(requestContext, firstRequest);
            assertEquals(1, firstResult.records().size());
            assertEquals(NONE.code(),
                firstResult.response().topics().get(0).partitions().get(0).errorCode());
            ctx.replay(firstResult.records());

            PartitionRegistration updatedPartition = replicationControl.getPartition(topicId, 0);
            InitDisklessLogRequestData secondRequest = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 200L, updatedPartition.leaderEpoch, List.of());

            ControllerResult<InitDisklessLogResponseData> secondResult =
                replicationControl.initDisklessLog(requestContext, secondRequest);

            assertEquals(0, secondResult.records().size());
            assertEquals(INVALID_REQUEST.code(),
                secondResult.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogNotSwitchPendingRejected() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            // The partition is not switch-pending (default -1), so InitDisklessLog must be rejected.
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);
            assertEquals(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET, partition.classicToDisklessStartOffset);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 100L, partition.leaderEpoch, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(0, result.records().size());
            assertEquals(INVALID_REQUEST.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogNonClassicTopicPartiallyInitialized() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic(
                "foo", 2, (short) 1, Map.of(DISKLESS_ENABLE_CONFIG, "true"), NONE.code());

            Uuid topicId = createTopicResult.topicId();
            markSwitchPending(ctx, topicId, 0);
            markSwitchPending(ctx, topicId, 1);
            PartitionRegistration partition0 = replicationControl.getPartition(topicId, 0);
            int leader0 = partition0.leader;

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);

            // Initialize partition 0
            InitDisklessLogRequestData firstRequest = singlePartitionRequest(
                leader0, defaultBrokerEpoch(leader0), topicId, 0, 100L, partition0.leaderEpoch, List.of());
            ControllerResult<InitDisklessLogResponseData> firstResult =
                replicationControl.initDisklessLog(requestContext, firstRequest);
            assertEquals(1, firstResult.records().size());
            assertEquals(NONE.code(),
                firstResult.response().topics().get(0).partitions().get(0).errorCode());
            ctx.replay(firstResult.records());

            // Try to re-initialize partition 0 — should be rejected because classicToDisklessStartOffset is already set
            PartitionRegistration updatedPartition0 = replicationControl.getPartition(topicId, 0);
            InitDisklessLogRequestData secondRequest = singlePartitionRequest(
                leader0, defaultBrokerEpoch(leader0), topicId, 0, 200L, updatedPartition0.leaderEpoch, List.of());
            ControllerResult<InitDisklessLogResponseData> secondResult =
                replicationControl.initDisklessLog(requestContext, secondRequest);
            assertEquals(0, secondResult.records().size());
            assertEquals(INVALID_REQUEST.code(),
                secondResult.response().topics().get(0).partitions().get(0).errorCode());

            // Partition 1 has not been initialized yet — should be accepted
            PartitionRegistration partition1 = replicationControl.getPartition(topicId, 1);
            int leader1 = partition1.leader;
            InitDisklessLogRequestData thirdRequest = singlePartitionRequest(
                leader1, defaultBrokerEpoch(leader1), topicId, 1, 300L, partition1.leaderEpoch, List.of());
            ControllerResult<InitDisklessLogResponseData> thirdResult =
                replicationControl.initDisklessLog(requestContext, thirdRequest);
            assertEquals(1, thirdResult.records().size());
            assertEquals(NONE.code(),
                thirdResult.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogExplicitClassicTopicAccepted() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic(
                "foo", 1, (short) 1, Map.of(DISKLESS_ENABLE_CONFIG, "false"), NONE.code());

            Uuid topicId = createTopicResult.topicId();
            markSwitchPending(ctx, topicId, 0);
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);
            int leaderId = partition.leader;

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                leaderId, defaultBrokerEpoch(leaderId), topicId, 0, 100L, partition.leaderEpoch, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(1, result.records().size());
            assertEquals(NONE.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());
        }

        @Test
        public void testInitDisklessLogMultipleProducerStates() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();
            markSwitchPending(ctx, topicId, 0);
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);

            List<InitDisklessLogRequestData.ProducerState> producerStates = List.of(
                new InitDisklessLogRequestData.ProducerState()
                    .setProducerId(1L)
                    .setProducerEpoch((short) 0)
                    .setBaseSequence(0)
                    .setLastSequence(10)
                    .setAssignedOffset(100L)
                    .setBatchMaxTimestamp(5000L),
                new InitDisklessLogRequestData.ProducerState()
                    .setProducerId(2L)
                    .setProducerEpoch((short) 3)
                    .setBaseSequence(5)
                    .setLastSequence(15)
                    .setAssignedOffset(200L)
                    .setBatchMaxTimestamp(6000L),
                new InitDisklessLogRequestData.ProducerState()
                    .setProducerId(3L)
                    .setProducerEpoch((short) 1)
                    .setBaseSequence(0)
                    .setLastSequence(0)
                    .setAssignedOffset(300L)
                    .setBatchMaxTimestamp(7000L)
            );

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 50L, partition.leaderEpoch, producerStates);

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(1, result.records().size());
            PartitionChangeRecord record = (PartitionChangeRecord) result.records().get(0).message();
            assertEquals(50L, InitDisklessLogFields.decodeClassicToDisklessStartOffset(record.unknownTaggedFields()));
            List<InitDisklessLogFields.ProducerStateEntry> decodedStates =
                InitDisklessLogFields.decodeProducerStates(record.unknownTaggedFields());
            assertEquals(3, decodedStates.size());

            assertEquals(NONE.code(), result.response().topics().get(0).partitions().get(0).errorCode());

            ctx.replay(result.records());
            PartitionRegistration updatedPartition = replicationControl.getPartition(topicId, 0);
            assertEquals(50L, updatedPartition.classicToDisklessStartOffset);
            assertEquals(3, updatedPartition.disklessProducerStates.size());
            assertEquals(1L, updatedPartition.disklessProducerStates.get(0).producerId());
            assertEquals((short) 0, updatedPartition.disklessProducerStates.get(0).producerEpoch());
            assertEquals(0, updatedPartition.disklessProducerStates.get(0).baseSequence());
            assertEquals(10, updatedPartition.disklessProducerStates.get(0).lastSequence());
            assertEquals(100L, updatedPartition.disklessProducerStates.get(0).assignedOffset());
            assertEquals(5000L, updatedPartition.disklessProducerStates.get(0).batchMaxTimestamp());
            assertEquals(2L, updatedPartition.disklessProducerStates.get(1).producerId());
            assertEquals(3L, updatedPartition.disklessProducerStates.get(2).producerId());
        }

        @Test
        public void testInitDisklessLogAcceptsSwitchPendingPartition() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}});

            Uuid topicId = createTopicResult.topicId();

            // Simulate switch pending by replaying a PartitionChangeRecord with -2
            PartitionChangeRecord switchPendingRecord = new PartitionChangeRecord()
                .setTopicId(topicId)
                .setPartitionId(0);
            switchPendingRecord.unknownTaggedFields().add(
                InitDisklessLogFields.encodeClassicToDisklessStartOffset(
                    PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING));
            ctx.replay(List.of(new ApiMessageAndVersion(switchPendingRecord, (short) 0)));

            PartitionRegistration pendingPartition = replicationControl.getPartition(topicId, 0);
            assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING, pendingPartition.classicToDisklessStartOffset);

            // InitDisklessLog should succeed even though classicToDisklessStartOffset is -2
            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData request = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 100L, pendingPartition.leaderEpoch, List.of());

            ControllerResult<InitDisklessLogResponseData> result =
                replicationControl.initDisklessLog(requestContext, request);

            assertEquals(1, result.records().size());
            assertEquals(NONE.code(),
                result.response().topics().get(0).partitions().get(0).errorCode());

            ctx.replay(result.records());
            PartitionRegistration updatedPartition = replicationControl.getPartition(topicId, 0);
            assertEquals(100L, updatedPartition.classicToDisklessStartOffset);
        }

        @Test
        public void testMarkClassicToDisklessSwitchStartedSuccess() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}, new int[] {1, 2, 0}});

            Uuid topicId = createTopicResult.topicId();

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            Map<ConfigResource, ApiError> configResults = Map.of(resource, ApiError.NONE);

            List<ApiMessageAndVersion> records =
                replicationControl.markClassicToDisklessSwitchStarted(configChanges, configResults);

            assertEquals(2, records.size());
            for (int i = 0; i < 2; i++) {
                assertInstanceOf(PartitionChangeRecord.class, records.get(i).message());
                PartitionChangeRecord record = (PartitionChangeRecord) records.get(i).message();
                assertEquals(topicId, record.topicId());
                assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING,
                    InitDisklessLogFields.decodeClassicToDisklessStartOffset(record.unknownTaggedFields()));
                // Leader must be set to force epoch bump on broker
                int partitionId = record.partitionId();
                int expectedLeader = replicationControl.getPartition(topicId, partitionId).leader;
                assertEquals(expectedLeader, record.leader());
            }

            int[] epochsBefore = new int[2];
            for (int i = 0; i < 2; i++) {
                epochsBefore[i] = replicationControl.getPartition(topicId, i).leaderEpoch;
            }

            ctx.replay(records);
            for (int i = 0; i < 2; i++) {
                PartitionRegistration partition = replicationControl.getPartition(topicId, i);
                assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING,
                    partition.classicToDisklessStartOffset);
                // Leader epoch must be bumped to trigger makeLeader on broker
                assertEquals(epochsBefore[i] + 1, partition.leaderEpoch);
            }
        }

        @Test
        public void testMarkClassicToDisklessSwitchStartedSkipsIneligibleChanges() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setDisklessStorageSystemEnabled(true)
                .build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("classic-topic", new int[][] {new int[] {0, 1, 2}});
            ctx.createTestTopic("already-diskless", 1, (short) 1,
                Map.of(DISKLESS_ENABLE_CONFIG, "true"), NONE.code());

            ConfigResource classicTopic = new ConfigResource(ConfigResource.Type.TOPIC, "classic-topic");
            ConfigResource alreadyDiskless = new ConfigResource(ConfigResource.Type.TOPIC, "already-diskless");
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            ConfigResource unknownTopic = new ConfigResource(ConfigResource.Type.TOPIC, "no-such-topic");

            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = new java.util.HashMap<>();
            // Config error on the topic
            configChanges.put(classicTopic, Map.of(DISKLESS_ENABLE_CONFIG,
                new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            // Already-diskless topic
            configChanges.put(alreadyDiskless, Map.of(DISKLESS_ENABLE_CONFIG,
                new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            // Non-TOPIC resource
            configChanges.put(brokerResource, Map.of(DISKLESS_ENABLE_CONFIG,
                new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            // Unknown topic
            configChanges.put(unknownTopic, Map.of(DISKLESS_ENABLE_CONFIG,
                new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            Map<ConfigResource, ApiError> configResults = new java.util.HashMap<>();
            configResults.put(classicTopic, new ApiError(Errors.INVALID_REQUEST, "bad config"));
            configResults.put(alreadyDiskless, ApiError.NONE);
            configResults.put(brokerResource, ApiError.NONE);
            configResults.put(unknownTopic, ApiError.NONE);

            List<ApiMessageAndVersion> records =
                replicationControl.markClassicToDisklessSwitchStarted(configChanges, configResults);
            assertEquals(0, records.size());

            // Also verify DELETE op and SET to "false" are skipped
            configChanges.clear();
            configResults.clear();
            configChanges.put(classicTopic, Map.of(DISKLESS_ENABLE_CONFIG,
                new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.DELETE, "true")));
            configResults.put(classicTopic, ApiError.NONE);

            records = replicationControl.markClassicToDisklessSwitchStarted(configChanges, configResults);
            assertEquals(0, records.size());

            configChanges.put(classicTopic, Map.of(DISKLESS_ENABLE_CONFIG,
                new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "false")));

            records = replicationControl.markClassicToDisklessSwitchStarted(configChanges, configResults);
            assertEquals(0, records.size());
        }

        @Test
        public void testMarkClassicToDisklessSwitchStartedSkipsAlreadyInitializedPartitions() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
            ReplicationControlManager replicationControl = ctx.replicationControl;
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2}, new int[] {1, 2, 0}});

            Uuid topicId = createTopicResult.topicId();
            markSwitchPending(ctx, topicId, 0);
            PartitionRegistration partition0 = replicationControl.getPartition(topicId, 0);

            ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
            InitDisklessLogRequestData initRequest = singlePartitionRequest(
                0, defaultBrokerEpoch(0), topicId, 0, 100L, partition0.leaderEpoch, List.of());
            ControllerResult<InitDisklessLogResponseData> initResult =
                replicationControl.initDisklessLog(requestContext, initRequest);
            ctx.replay(initResult.records());

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            Map<ConfigResource, ApiError> configResults = Map.of(resource, ApiError.NONE);

            List<ApiMessageAndVersion> records =
                replicationControl.markClassicToDisklessSwitchStarted(configChanges, configResults);

            // Only partition 1 should be marked — partition 0 was already initialized
            assertEquals(1, records.size());
            PartitionChangeRecord record = (PartitionChangeRecord) records.get(0).message();
            assertEquals(topicId, record.topicId());
            assertEquals(1, record.partitionId());
            assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING,
                InitDisklessLogFields.decodeClassicToDisklessStartOffset(record.unknownTaggedFields()));
        }

        @Test
        public void testSwitchRejectedWhenPartitionIsOffline() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            // Fence all brokers to make the partition offline
            ctx.fenceBrokers(0, 1, 2);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertTrue(error.message().contains("offline"), "Expected 'offline' in: " + error.message());
        }

        @Test
        public void testSwitchRejectedWhenReassignmentInProgress() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            ctx.registerBrokers(0, 1, 2, 3);
            ctx.unfenceBrokers(0, 1, 2, 3);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            // Start a reassignment
            ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
                ctx.replicationControl.alterPartitionReassignments(
                    new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new ReassignableTopic().setName("foo").setPartitions(List.of(
                            new ReassignablePartition().setPartitionIndex(0).
                                setReplicas(List.of(1, 2, 3)))))));
            ctx.replay(alterResult.records());

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertTrue(error.message().contains("reassignment"), "Expected 'reassignment' in: " + error.message());
        }

        @Test
        public void testSwitchRejectedWhenUnderReplicated() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();

            // Shrink ISR to make it under-replicated (fence one broker then unfence it without rejoining ISR)
            ctx.fenceBrokers(2);
            // Now partition has ISR < replicas but still has a leader
            PartitionRegistration partition = ctx.replicationControl.getPartition(fooId, 0);
            assertTrue(partition.hasLeader());
            assertTrue(partition.isr.length < partition.replicas.length);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertTrue(error.message().contains("under-replicated"), "Expected 'under-replicated' in: " + error.message());
        }

        @Test
        public void testSwitchRejectedWhenElrIsNonEmpty() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
                .setIsElrEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();

            // Fence broker 2 — ISR drops below minISR (3), so broker 2 goes to ELR
            ctx.fenceBrokers(2);
            PartitionRegistration partition = ctx.replicationControl.getPartition(fooId, 0);
            assertTrue(partition.elr.length > 0,
                "Expected ELR to be non-empty after fencing with minISR=3, got elr=" +
                Arrays.toString(partition.elr));

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertTrue(error.message().contains("non-empty ELR"),
                "Expected 'non-empty ELR' in: " + error.message());
        }

        @Test
        public void testSwitchRejectedWhenLastKnownElrIsNonEmpty() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
                .setIsElrEnabled(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();

            // Fence broker 2 — ISR drops below minISR (3), so broker 2 goes to ELR
            ctx.fenceBrokers(2);
            PartitionRegistration partition = ctx.replicationControl.getPartition(fooId, 0);
            assertTrue(partition.elr.length > 0 || partition.lastKnownElr.length > 0,
                "Expected ELR or lastKnownElr to be non-empty after fencing, got elr=" +
                Arrays.toString(partition.elr) + " lastKnownElr=" + Arrays.toString(partition.lastKnownElr));

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertTrue(error.message().contains("ELR") || error.message().contains("last-known ELR"),
                "Expected 'ELR' or 'last-known ELR' in: " + error.message());
        }

        @Test
        public void testSwitchRejectedWhenRecovering() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();

            // Fence brokers 1, 2 to shrink ISR to [0]
            ctx.fenceBrokers(1, 2);
            // Fence broker 0 to make partition leaderless
            ctx.fenceBrokers(0, 1, 2);
            // Unfence broker 1 to trigger unclean election (RECOVERING state)
            ctx.unfenceBrokers(1);

            PartitionRegistration partition = ctx.replicationControl.getPartition(fooId, 0);
            assertEquals(LeaderRecoveryState.RECOVERING, partition.leaderRecoveryState);

            // Disable unclean leader election so the unclean check doesn't fire first
            ctx.replay(ctx.configurationControl.incrementalAlterConfigs(
                Map.of(new ConfigResource(ConfigResource.Type.TOPIC, "foo"),
                    Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                        new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "false"))),
                false).records());

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertTrue(error.message().contains("recovering"),
                "Expected 'recovering' in: " + error.message());
        }

        @Test
        public void testMaybeTriggerUncleanElectionSkipsPendingSwitchPartition() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();

            // Shrink ISR to just [0] by fencing brokers 1 and 2, then unfence them
            // so they are unfenced replicas (non-ISR) eligible for unclean election.
            ctx.fenceBrokers(1, 2);
            ctx.unfenceBrokers(1, 2);
            PartitionRegistration partitionBefore = ctx.replicationControl.getPartition(fooId, 0);
            assertEquals(0, partitionBefore.leader);
            assertEquals(1, partitionBefore.isr.length, "ISR should be shrunk to just the leader");

            // Mark the partition as switch pending
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> disklessChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            List<ApiMessageAndVersion> switchRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStarted(
                    disklessChanges, Map.of(resource, ApiError.NONE));
            ctx.replay(switchRecords);

            // Fence the leader (broker 0) — partition becomes leaderless.
            // Brokers 1 and 2 are unfenced replicas eligible for unclean election.
            ctx.fenceBrokers(0);
            PartitionRegistration partitionAfterFence = ctx.replicationControl.getPartition(fooId, 0);
            assertFalse(partitionAfterFence.hasLeader(),
                "Partition should be leaderless because the pending-switch guard " +
                "prevented unclean election during fencing");

            // Explicitly call maybeTriggerUncleanLeaderElection — should also be skipped
            List<ApiMessageAndVersion> electionRecords = new ArrayList<>();
            ctx.replicationControl.maybeTriggerUncleanLeaderElectionForLeaderlessPartitions(
                electionRecords, Integer.MAX_VALUE);

            assertEquals(0, electionRecords.size(),
                "Expected no unclean election for partition with pending switch");
        }

        @Test
        public void testBrokerFencingDoesNotTriggerUncleanElectionForPendingSwitchPartition() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();

            // Shrink ISR to just [0] by fencing brokers 1 and 2, then unfence them
            // so they are replicas but not in ISR.
            ctx.fenceBrokers(1, 2);
            ctx.unfenceBrokers(1, 2);
            PartitionRegistration partitionBefore = ctx.replicationControl.getPartition(fooId, 0);
            assertEquals(0, partitionBefore.leader);
            assertEquals(1, partitionBefore.isr.length, "ISR should be shrunk to just the leader");

            // Mark the partition as switch pending
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> disklessChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            List<ApiMessageAndVersion> switchRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStarted(
                    disklessChanges, Map.of(resource, ApiError.NONE));
            ctx.replay(switchRecords);

            // Fence the leader (broker 0) — brokers 1, 2 are unfenced replicas (non-ISR).
            // With unclean enabled but pending switch, should NOT do unclean election.
            ctx.fenceBrokers(0);
            PartitionRegistration partition = ctx.replicationControl.getPartition(fooId, 0);

            // Partition should be leaderless, not unclean-elected
            assertFalse(partition.hasLeader(),
                "Expected no unclean election for partition with pending switch");
        }

        @Test
        public void testLegacyAlterConfigsRejectsImplicitDisklessEnableDeletion() {
            // Legacy AlterConfigs replaces the entire config map. If a topic has
            // diskless.enable=false and the request omits it, the override would be deleted,
            // switching to diskless via broker default. This must be rejected like an
            // incremental DELETE of diskless.enable — a switch must be an explicit diskless.enable=true.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false"), (short) 0);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            assertEquals("false", ctx.configurationControl.currentTopicConfig("foo").get(DISKLESS_ENABLE_CONFIG));

            // Legacy AlterConfigs with only retention.ms (omits diskless.enable).
            // Since this would delete diskless.enable=false, it must be rejected.
            Map<ConfigResource, Map<String, String>> newConfigs =
                Map.of(resource, Map.of("retention.ms", "86400000"));
            ControllerResult<Map<ConfigResource, ApiError>> legacyResult =
                ctx.configurationControl.legacyAlterConfigs(newConfigs, false,
                    r -> ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(
                        r, newConfigs));

            assertEquals(Errors.INVALID_CONFIG, legacyResult.response().get(resource).error(),
                "Legacy AlterConfigs should reject implicit diskless.enable deletion");
            assertTrue(legacyResult.response().get(resource).message().contains("not allowed to delete"),
                "Expected delete rejection in: " + legacyResult.response().get(resource).message());
            assertTrue(legacyResult.records().isEmpty(),
                "Rejected legacy AlterConfigs must not emit config records");

            List<ApiMessageAndVersion> switchRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStartedForLegacyAlterConfigs(
                    newConfigs, legacyResult.response());
            assertTrue(switchRecords.isEmpty(),
                "Rejected implicit diskless deletion must not emit switch-pending records");
        }

        @Test
        public void testLegacyAlterConfigsRejectsExplicitDisklessEnableNullDeletion() {
            // The legacy AlterConfigs wire format allows null values (AlterConfigsRequest.json
            // nullableVersions: "0+"). An explicit diskless.enable=null lands in
            // recordsExplicitlyAltered and must be rejected, same as an implicit omission.
            // Without this guard, the deletion would silently remove the diskless.enable override;
            // with defaultDisklessEnable=true the controller would treat the topic as diskless
            // without RS injection or switch-pending markers.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false"), (short) 0);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            assertEquals("false", ctx.configurationControl.currentTopicConfig("foo").get(DISKLESS_ENABLE_CONFIG));

            // Legacy AlterConfigs with diskless.enable=null (explicit null, not omission).
            Map<String, String> explicitNull = new HashMap<>();
            explicitNull.put(DISKLESS_ENABLE_CONFIG, null);
            explicitNull.put("retention.ms", "86400000");
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, explicitNull);
            ControllerResult<Map<ConfigResource, ApiError>> legacyResult =
                ctx.configurationControl.legacyAlterConfigs(newConfigs, false,
                    r -> ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(
                        r, newConfigs));

            assertEquals(Errors.INVALID_CONFIG, legacyResult.response().get(resource).error(),
                "Legacy AlterConfigs should reject explicit diskless.enable=null");
            assertTrue(legacyResult.response().get(resource).message().contains("not allowed to delete"),
                "Expected delete rejection in: " + legacyResult.response().get(resource).message());
            assertTrue(legacyResult.records().isEmpty(),
                "Rejected request must not emit config records");

            List<ApiMessageAndVersion> switchRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStartedForLegacyAlterConfigs(
                    newConfigs, legacyResult.response());
            assertTrue(switchRecords.isEmpty(),
                "Rejected explicit null diskless deletion must not emit switch-pending records");
        }

        @Test
        public void testElectLeadersRejectsUncleanElectionForPendingSwitchPartition() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();

            // Shrink ISR to just [0] by fencing brokers 1 and 2, then unfence them
            ctx.fenceBrokers(1, 2);
            ctx.unfenceBrokers(1, 2);
            PartitionRegistration partitionBefore = ctx.replicationControl.getPartition(fooId, 0);
            assertEquals(0, partitionBefore.leader);
            assertEquals(1, partitionBefore.isr.length);

            // Mark the partition as switch pending
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> disklessChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            List<ApiMessageAndVersion> switchRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStarted(
                    disklessChanges, Map.of(resource, ApiError.NONE));
            ctx.replay(switchRecords);

            // Fence the leader to make partition leaderless
            ctx.fenceBrokers(0);
            PartitionRegistration partitionAfterFence = ctx.replicationControl.getPartition(fooId, 0);
            assertFalse(partitionAfterFence.hasLeader());

            // Attempt explicit unclean election via electLeaders API — should be rejected
            ElectLeadersRequestData request = new ElectLeadersRequestData()
                .setElectionType(ElectionType.UNCLEAN.value);
            request.topicPartitions().add(new TopicPartitions()
                .setTopic("foo")
                .setPartitions(List.of(0)));

            ControllerResult<ElectLeadersResponseData> result =
                ctx.replicationControl.electLeaders(request);

            assertEquals(0, result.records().size(),
                "Expected no election records for partition with pending switch");

            ReplicaElectionResult topicResult = result.response().replicaElectionResults().get(0);
            assertEquals("foo", topicResult.topic());
            PartitionResult partitionResult = topicResult.partitionResult().get(0);
            assertEquals(0, partitionResult.partitionId());
            assertEquals(Errors.INVALID_REQUEST.code(), partitionResult.errorCode());
            assertTrue(partitionResult.errorMessage().contains("pending classic-to-diskless switch"),
                "Expected pending switch message in: " + partitionResult.errorMessage());
        }

        @Test
        public void testSwitchRejectedWhenUncleanLeaderElectionAlreadyEnabled() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot switch topic foo to diskless: " +
                "unclean leader election must be disabled.", error.message());
        }

        @Test
        public void testSwitchRejectedWhenUncleanLeaderElectionBeingEnabled() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(
                    DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"),
                    TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot switch topic foo to diskless: " +
                "unclean leader election must be disabled.", error.message());
        }

        @Test
        public void testSwitchRejectedWhenUncleanLeaderElectionOverrideDeleted() {
            // Topic has unclean=false override, but cluster default is true.
            // Deleting the override would revert to the cluster default — must reject.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false"), (short) 0);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(
                    DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"),
                    TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.DELETE, null)));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot switch topic foo to diskless: " +
                "unclean leader election must be disabled.", error.message());
        }

        @Test
        public void testSwitchAllowedWhenUncleanLeaderElectionDisabled() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(ApiError.NONE, error);
        }

        @Test
        public void testUncleanLeaderElectionDeleteRejectedWhenPendingSwitch() {
            // Topic has unclean=false override, cluster default is true.
            // The topic already has a pending classic-to-diskless switch.
            // An incremental DELETE of the override would revert to true.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false"), (short) 0);

            // Put the topic in pending-switch state
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> disklessChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            List<ApiMessageAndVersion> switchRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStarted(
                    disklessChanges, Map.of(resource, ApiError.NONE));
            ctx.replay(switchRecords);

            // Now attempt an incremental DELETE of the unclean override
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.DELETE, "")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot enable unclean leader election for topic foo" +
                ": topic has a pending classic-to-diskless switch.", error.message());
        }

        @Test
        public void testUncleanLeaderElectionSetRejectedWhenPendingSwitch() {
            // Topic has a pending classic-to-diskless switch.
            // An incremental SET of unclean=true should be rejected.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            // Put the topic in pending-switch state
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> disklessChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            List<ApiMessageAndVersion> switchRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStarted(
                    disklessChanges, Map.of(resource, ApiError.NONE));
            ctx.replay(switchRecords);

            // Now attempt to SET unclean=true
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot enable unclean leader election for topic foo" +
                ": topic has a pending classic-to-diskless switch.", error.message());
        }

        @Test
        public void testUncleanLeaderElectionDeleteRejectedWhenAlreadyDisklessButPendingSwitch() {
            // Topic is already diskless (config says diskless.enable=true) but the switch
            // is still pending (classicToDisklessStartOffset == PENDING).
            // Deleting the unclean=false override should still be rejected.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false"), (short) 0).topicId();

            // Mark the topic as diskless and simulate pending switch
            ctx.alterTopicConfig("foo", DISKLESS_ENABLE_CONFIG, "true");
            PartitionChangeRecord switchPendingRecord = new PartitionChangeRecord()
                .setTopicId(fooId)
                .setPartitionId(0);
            switchPendingRecord.unknownTaggedFields().add(
                InitDisklessLogFields.encodeClassicToDisklessStartOffset(
                    PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING));
            ctx.replay(List.of(new ApiMessageAndVersion(switchPendingRecord, (short) 0)));

            // Now attempt an incremental DELETE of the unclean override
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.DELETE, "")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot enable unclean leader election for topic foo" +
                ": topic has a pending classic-to-diskless switch.", error.message());
        }

        @Test
        public void testIncrementalDeleteDisklessEnableRejectedWhenUnderReplicated() {
            // Simulates removal of a diskless.enable=false override when the broker
            // default is true. Note: ConfigurationControlManager rejects DELETE for
            // diskless.enable before this validator runs, so this exercises the
            // validator in isolation for defense-in-depth.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false"), (short) 0).topicId();

            // Make the partition under-replicated
            ctx.fenceBrokers(2);
            PartitionRegistration partition = ctx.replicationControl.getPartition(fooId, 0);
            assertTrue(partition.isr.length < partition.replicas.length);

            // Removing diskless.enable override reverts to broker default (true),
            // triggering a switch — rejected because partition is under-replicated
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.DELETE, "")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertTrue(error.message().contains("under-replicated"),
                "Expected 'under-replicated' in: " + error.message());
        }

        @Test
        public void testLegacySwitchRejectedWhenUncleanLeaderElectionExplicitlyEnabled() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false"), (short) 0);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, Map.of(
                DISKLESS_ENABLE_CONFIG, "true",
                TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true"));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(resource, newConfigs);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot switch topic foo to diskless: " +
                "unclean leader election must be disabled.", error.message());
        }

        @Test
        public void testLegacySwitchRejectedWhenUncleanLeaderElectionEnabledFromDefaults() {
            // Topic has unclean=false override, cluster default is true.
            // Legacy alter omits the key — override is removed, effective value reverts to true.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false",
                    TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false"), (short) 0);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            // Legacy alter with only diskless.enable=true — unclean override is removed.
            // Since the broker default enables unclean leader election, it should be rejected.
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, Map.of(
                DISKLESS_ENABLE_CONFIG, "true"));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(resource, newConfigs);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot switch topic foo to diskless: " +
                "unclean leader election must be disabled.", error.message());
        }

        @Test
        public void testLegacySwitchRejectedWhenAlreadyDisklessButPendingSwitch() {
            // Topic is already diskless but the switch is still pending.
            // Legacy alter with unclean=true should be rejected.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false"), (short) 0).topicId();

            // Mark the topic as diskless and simulate pending switch
            ctx.alterTopicConfig("foo", DISKLESS_ENABLE_CONFIG, "true");
            PartitionChangeRecord switchPendingRecord = new PartitionChangeRecord()
                .setTopicId(fooId)
                .setPartitionId(0);
            switchPendingRecord.unknownTaggedFields().add(
                InitDisklessLogFields.encodeClassicToDisklessStartOffset(
                    PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING));
            ctx.replay(List.of(new ApiMessageAndVersion(switchPendingRecord, (short) 0)));

            // Legacy alter that sets diskless.enable=true and omits the unclean override
            // — effective unclean reverts to cluster default (true).
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, Map.of(
                DISKLESS_ENABLE_CONFIG, "true"));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(resource, newConfigs);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot enable unclean leader election for topic foo" +
                ": topic has a pending classic-to-diskless switch.", error.message());
        }

        @Test
        public void testLegacyUncleanOnlyRejectedWhenPendingSwitch() {
            // Topic is already diskless with a pending switch.
            // Legacy alter sends only unclean=true (omits diskless.enable).
            // Since legacy is a full replacement, omitting diskless.enable
            // means it will be deleted — but the pending-switch guard should
            // still reject enabling unclean.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false"), (short) 0).topicId();

            // Switch the topic to diskless and mark as pending
            ctx.alterTopicConfig("foo", DISKLESS_ENABLE_CONFIG, "true");
            PartitionChangeRecord switchPendingRecord = new PartitionChangeRecord()
                .setTopicId(fooId)
                .setPartitionId(0);
            switchPendingRecord.unknownTaggedFields().add(
                InitDisklessLogFields.encodeClassicToDisklessStartOffset(
                    PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING));
            ctx.replay(List.of(new ApiMessageAndVersion(switchPendingRecord, (short) 0)));

            // Legacy alter that only sets unclean=true, omitting diskless.enable entirely.
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, Map.of(
                TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true"));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(resource, newConfigs);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot enable unclean leader election for topic foo" +
                ": topic has a pending classic-to-diskless switch.", error.message());
        }

        @Test
        public void testLegacyUncleanSetRejectedWhenPendingSwitch() {
            // Topic has a pending classic-to-diskless switch.
            // Legacy alter with diskless.enable=true and unclean=true should be rejected.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            // Put the topic in pending-switch state
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> disklessChanges = Map.of(
                resource, Map.of(DISKLESS_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
            List<ApiMessageAndVersion> switchRecords =
                ctx.replicationControl.markClassicToDisklessSwitchStarted(
                    disklessChanges, Map.of(resource, ApiError.NONE));
            ctx.replay(switchRecords);
            ctx.alterTopicConfig("foo", DISKLESS_ENABLE_CONFIG, "true");

            // Legacy alter with both diskless=true and unclean=true
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, Map.of(
                DISKLESS_ENABLE_CONFIG, "true",
                TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true"));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(resource, newConfigs);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot enable unclean leader election for topic foo" +
                ": topic has a pending classic-to-diskless switch.", error.message());
        }

        @Test
        public void testLegacyNullValueTreatedAsDeletion() {
            // Legacy AlterConfigs can send null values meaning "delete this override".
            // A null unclean.leader.election.enable with a cluster default of true
            // should be treated as deletion — effective unclean reverts to true.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false"), (short) 0);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            // null value for unclean means "delete override" → reverts to cluster default (true)
            HashMap<String, String> configs = new HashMap<>();
            configs.put(DISKLESS_ENABLE_CONFIG, "true");
            configs.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, null);
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, configs);

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(resource, newConfigs);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot switch topic foo to diskless: " +
                "unclean leader election must be disabled.", error.message());
        }

        @Test
        public void testUncleanAllowedOnFullyDisklessTopic() {
            // Topic is fully diskless (switch completed, no pending partitions).
            // Enabling unclean should be allowed. An out-of-sync replica can
            // discover state from there.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

            // Make the topic fully diskless (no pending switch)
            ctx.alterTopicConfig("foo", DISKLESS_ENABLE_CONFIG, "true");

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
                resource, Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                    new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPrecondition(resource, configChanges);

            assertEquals(ApiError.NONE, error);
        }

        @Test
        public void testLegacyOmittingBothConfigsRejectedWhenPendingSwitchAndUncleanDefault() {
            // Topic has overrides: diskless.enable=true, unclean=false.
            // Cluster default for unclean is true.
            // The topic has a pending switch.
            // Legacy alter omits both configs — this deletes both overrides,
            // reverting unclean to the true default. Should be rejected.
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false",
                    TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false"), (short) 0).topicId();

            // Switch the topic to diskless and mark as pending
            ctx.alterTopicConfig("foo", DISKLESS_ENABLE_CONFIG, "true");
            PartitionChangeRecord switchPendingRecord = new PartitionChangeRecord()
                .setTopicId(fooId)
                .setPartitionId(0);
            switchPendingRecord.unknownTaggedFields().add(
                InitDisklessLogFields.encodeClassicToDisklessStartOffset(
                    PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING));
            ctx.replay(List.of(new ApiMessageAndVersion(switchPendingRecord, (short) 0)));

            // Legacy alter with empty config map — deletes all topic overrides
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, Map.of());

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(resource, newConfigs);

            assertEquals(Errors.INVALID_CONFIG, error.error());
            assertEquals("Cannot enable unclean leader election for topic foo" +
                ": topic has a pending classic-to-diskless switch.", error.message());
        }

        @Test
        public void testLegacySwitchAllowedWhenUncleanLeaderElectionDisabled() {
            ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
                .setDisklessStorageSystemEnabled(true)
                .setDefaultDisklessEnable(true)
                .build();
            ctx.registerBrokers(0, 1, 2);
            ctx.unfenceBrokers(0, 1, 2);
            ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
                Map.of(DISKLESS_ENABLE_CONFIG, "false"), (short) 0);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
            Map<ConfigResource, Map<String, String>> newConfigs = Map.of(resource, Map.of(
                DISKLESS_ENABLE_CONFIG, "true"));

            ApiError error =
                ctx.replicationControl.validateClassicToDisklessSwitchPreconditionForLegacy(resource, newConfigs);

            assertEquals(ApiError.NONE, error);
        }

    }

    @Test
    public void testAlterDisklessSwitchForcesSeal() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        Uuid topicId = createSwitchingTestTopic(ctx);

        ControllerResult<AlterDisklessSwitchResponseData> result =
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(0).setSealOffset(100L));
        assertEquals((short) 0, result.response().errorCode());
        ctx.replay(result.records());

        PartitionRegistration partition = replicationControl.getPartition(topicId, 0);
        assertEquals(100L, partition.classicToDisklessStartOffset);
        // The current leader epoch is captured as the diskless leader epoch.
        assertEquals(partition.leaderEpoch, partition.disklessLeaderEpoch);
        // By default a forced seal leaves producer states untouched: no producer-states tag is written.
        PartitionChangeRecord record = (PartitionChangeRecord) result.records().get(0).message();
        assertTrue(InitDisklessLogFields.decodeProducerStatesIfPresent(record.unknownTaggedFields()).isEmpty());
    }

    @Test
    public void testAlterDisklessSwitchForcesSealClearingProducerStates() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        createSwitchingTestTopic(ctx);

        ControllerResult<AlterDisklessSwitchResponseData> result =
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(0).setSealOffset(100L).setClearProducerStates(true));
        assertEquals((short) 0, result.response().errorCode());

        // With clearProducerStates the record carries an explicit empty producer-states tag so merge() clears them.
        PartitionChangeRecord record = (PartitionChangeRecord) result.records().get(0).message();
        assertEquals(List.of(), InitDisklessLogFields.decodeProducerStatesIfPresent(record.unknownTaggedFields())
            .orElseThrow(() -> new AssertionError("expected an explicit producer-states tag")));
    }

    @Test
    public void testAlterDisklessSwitchAbortsSwitch() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        Uuid topicId = createSwitchingTestTopic(ctx);

        // Abort the pending switch back to classic.
        ctx.replay(replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
            .setTopicName("foo").setPartitionIndex(0).setSealOffset(-1L)).records());
        PartitionRegistration aborted = replicationControl.getPartition(topicId, 0);
        assertEquals(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET, aborted.classicToDisklessStartOffset);
        assertEquals(List.of(), aborted.disklessProducerStates);
        assertEquals(PartitionRegistration.NO_DISKLESS_LEADER_EPOCH, aborted.disklessLeaderEpoch);
    }

    @Test
    public void testAlterDisklessSwitchReArms() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        Uuid topicId = createSwitchingTestTopic(ctx);
        int leaderEpochBefore = replicationControl.getPartition(topicId, 0).leaderEpoch;

        // Re-arm the pending switch.
        ctx.replay(replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
            .setTopicName("foo").setPartitionIndex(0).setSealOffset(-2L)).records());

        PartitionRegistration partition = replicationControl.getPartition(topicId, 0);
        assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING, partition.classicToDisklessStartOffset);
        // Re-arming bumps the leader epoch to force the broker to seal again.
        assertEquals(leaderEpochBefore + 1, partition.leaderEpoch);
    }

    @Test
    public void testAlterDisklessSwitchCannotAbortCommittedSeal() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        createSwitchingTestTopic(ctx);

        // Commit a seal; diskless data may now exist past it, so abort/re-arm must be rejected.
        ctx.replay(replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
            .setTopicName("foo").setPartitionIndex(0).setSealOffset(100L)).records());

        assertThrows(InvalidRequestException.class, () ->
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(0).setSealOffset(-1L)));
        assertThrows(InvalidRequestException.class, () ->
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(0).setSealOffset(-2L)));
    }

    @Test
    public void testAlterDisklessSwitchCannotReSealBeyondCommittedSeal() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        Uuid topicId = createSwitchingTestTopic(ctx);

        // Commit a seal at 100. The classic log is truncated to the seal, so 100 is its end offset.
        ctx.replay(replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
            .setTopicName("foo").setPartitionIndex(0).setSealOffset(100L)).records());

        // Re-sealing beyond the committed seal would route non-existent classic offsets and is rejected.
        assertThrows(InvalidRequestException.class, () ->
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(0).setSealOffset(150L)));

        // Re-sealing at or below the committed seal is allowed (correcting a bad seal downward).
        ctx.replay(replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
            .setTopicName("foo").setPartitionIndex(0).setSealOffset(50L)).records());
        assertEquals(50L, replicationControl.getPartition(topicId, 0).classicToDisklessStartOffset);
    }

    @Test
    public void testAlterDisklessSwitchRejectsInvalidOffset() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        createSwitchingTestTopic(ctx);

        assertThrows(InvalidRequestException.class, () ->
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(0).setSealOffset(-3L)));
    }

    @Test
    public void testAlterDisklessSwitchRejectsClassicTopics() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});

        assertThrows(InvalidRequestException.class, () ->
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(0).setSealOffset(100L)));
    }

    @Test
    public void testAlterDisklessSwitchRejectsPartitionNotInSwitch() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        // Born-diskless topic: diskless.enable=true but never part of a switch (classicToDisklessStartOffset=-1).
        CreateTopicsRequestData.CreatableTopicConfigCollection configs =
            new CreateTopicsRequestData.CreatableTopicConfigCollection();
        configs.add(new CreateTopicsRequestData.CreatableTopicConfig()
            .setName(DISKLESS_ENABLE_CONFIG).setValue("true"));
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic()
            .setName("foo").setNumPartitions(-1).setReplicationFactor((short) -1).setConfigs(configs));
        ctx.replay(replicationControl.createTopics(
            anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Set.of("foo")).records());

        assertThrows(InvalidRequestException.class, () ->
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(0).setSealOffset(100L)));
    }

    @Test
    public void testAlterDisklessSwitchRejectsUnknownTopic() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;

        assertThrows(UnknownTopicOrPartitionException.class, () ->
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("nonexistent").setPartitionIndex(0).setSealOffset(100L)));
    }

    @Test
    public void testAlterDisklessSwitchRejectsUnknownPartition() {
        ReplicationControlTestContext ctx = disklessSwitchTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        createSwitchingTestTopic(ctx);

        assertThrows(UnknownTopicOrPartitionException.class, () ->
            replicationControl.alterDisklessSwitch(new AlterDisklessSwitchRequestData()
                .setTopicName("foo").setPartitionIndex(5).setSealOffset(100L)));
    }

    private static ReplicationControlTestContext disklessSwitchTestContext() {
        return new ReplicationControlTestContext.Builder()
            .setStaticConfig(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, true)
            .setDisklessStorageSystemEnabled(true)
            .build();
    }

    private static Uuid createSwitchingTestTopic(ReplicationControlTestContext ctx) {
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid topicId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
            Map.of(DISKLESS_ENABLE_CONFIG, "false"), (short) 0).topicId();

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
        Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges = Map.of(
            resource, Map.of(DISKLESS_ENABLE_CONFIG,
                new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true")));
        List<ApiMessageAndVersion> switchRecords = ctx.replicationControl.markClassicToDisklessSwitchStarted(
            configChanges, Map.of(resource, ApiError.NONE));
        ctx.replay(ctx.configurationControl.incrementalAlterConfigs(configChanges, true).records());
        ctx.replay(switchRecords);
        return topicId;
    }
}
