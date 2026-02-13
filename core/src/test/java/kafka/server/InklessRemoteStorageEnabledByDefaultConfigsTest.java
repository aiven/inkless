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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlane;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlaneConfig;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.storage_backend.s3.S3StorageConfig;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.MinioContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;
import io.aiven.inkless.test_utils.S3TestContainer;

import static org.apache.kafka.common.config.TopicConfig.DISKLESS_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class InklessRemoteStorageEnabledByDefaultConfigsTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    @BeforeEach
    public void setup(final TestInfo testInfo) {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
    }

    @Nested
    class CreateTopic {
        @Test
        void creatingClassicTopicsAlwaysEnablesRemoteStorage() throws Exception {
            // Given a cluster with default.remote.storage.for.topic.create.enable=true
            var cluster = initWithRemoteDefaultEnabled();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // When creating a topic without specifying remote.storage.enable
                var result1 = createTopic(admin, "classicWithoutRemoteStorage", Map.of());
                // Then a topic with remote.storage.enable=true and diskless.enable=false is created
                assertEquals("true", result1.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", result1.get(DISKLESS_ENABLE_CONFIG));
                var classicWithoutRemoteStorageConfigs = getTopicConfig(admin, "classicWithoutRemoteStorage");
                assertEquals("true", classicWithoutRemoteStorageConfigs.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", classicWithoutRemoteStorageConfigs.get(DISKLESS_ENABLE_CONFIG));

                // When creating a topic by specifying remote.storage.enable=true
                var result2 = createTopic(admin, "classicWithRemoteStorageTrue", Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"));
                // Then a topic with remote.storage.enable=true and diskless.enable=false is created
                assertEquals("true", result2.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", result2.get(DISKLESS_ENABLE_CONFIG));
                var classicWithRemoteStorageTrueConfigs = getTopicConfig(admin, "classicWithRemoteStorageTrue");
                assertEquals("true", classicWithRemoteStorageTrueConfigs.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", classicWithRemoteStorageTrueConfigs.get(DISKLESS_ENABLE_CONFIG));

                // When creating a topic by specifying remote.storage.enable=false
                var result3 = createTopic(admin, "classicWithRemoteStorageFalse", Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"));
                // Then a topic with remote.storage.enable=true and diskless.enable=false is created
                assertEquals("true", result3.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", result3.get(DISKLESS_ENABLE_CONFIG));
                var classicWithRemoteStorageFalseConfigs = getTopicConfig(admin, "classicWithRemoteStorageFalse");
                assertEquals("true", classicWithRemoteStorageFalseConfigs.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", classicWithRemoteStorageFalseConfigs.get(DISKLESS_ENABLE_CONFIG));
            } finally {
                cluster.close();
            }
        }

        @Test
        void creatingDisklessTopicsAlwaysDisablesRemoteStorage() throws Exception {
            // Given a cluster with default.remote.storage.for.topic.create.enable=true
            var cluster = initWithRemoteDefaultEnabled();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // When creating a diskless topic without specifying remote.storage.enable
                var result1 = createTopic(admin, "disklessWithoutRemoteStorage", Map.of(DISKLESS_ENABLE_CONFIG, "true"));
                // Then a topic with diskless.enable=true and remote.storage.enable=false is created
                assertEquals("true", result1.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", result1.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                var disklessWithoutRemoteStorageConfigs = getTopicConfig(admin, "disklessWithoutRemoteStorage");
                assertEquals("true", disklessWithoutRemoteStorageConfigs.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", disklessWithoutRemoteStorageConfigs.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // When creating a diskless topic by specifying remote.storage.enable=true
                var result2 = createTopic(admin, "disklessWithRemoteStorageTrue", Map.of(
                    DISKLESS_ENABLE_CONFIG, "true",
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
                ));
                // Then a topic with diskless.enable=true and remote.storage.enable=false is created
                assertEquals("true", result2.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", result2.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                var disklessWithRemoteStorageTrueConfig = getTopicConfig(admin, "disklessWithRemoteStorageTrue");
                assertEquals("true", disklessWithRemoteStorageTrueConfig.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", disklessWithRemoteStorageTrueConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // When creating a diskless topic by specifying remote.storage.enable=false
                var result3 = createTopic(admin, "disklessWithRemoteStorageFalse", Map.of(
                    DISKLESS_ENABLE_CONFIG, "true",
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"
                ));
                // Then a topic with diskless.enable=true and remote.storage.enable=false is created
                assertEquals("true", result3.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", result3.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                var disklessWithRemoteStorageFalseConfig = getTopicConfig(admin, "disklessWithRemoteStorageFalse");
                assertEquals("true", disklessWithRemoteStorageFalseConfig.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", disklessWithRemoteStorageFalseConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
            } finally {
                cluster.close();
            }
        }

        @Test
        void creatingConfiguredLocalOnlyTopicDisablesRemoteStorage() throws Exception {
            // Given a cluster with remote storage default enabled and a custom local-only topic regex
            var cluster = initWithRemoteDefaultEnabled("custom-schema-topic(-.*)?");
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // When creating a topic matching the configured local-only regex
                var localOnlyResult = createTopic(admin, "custom-schema-topic-prod", Map.of());
                // Then remote storage is disabled
                assertEquals("false", localOnlyResult.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", localOnlyResult.get(DISKLESS_ENABLE_CONFIG));

                // And regular topics still default to remote storage enabled
                var regularTopicResult = createTopic(admin, "regularTopic", Map.of());
                assertEquals("true", regularTopicResult.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", regularTopicResult.get(DISKLESS_ENABLE_CONFIG));
            } finally {
                cluster.close();
            }
        }
    }

    @Nested
    class IncrementalAlterConfigs {
        @Test
        void alteringConfigOfClassicTopicReturnsError() throws Exception {
            // Given a cluster with default.remote.storage.for.topic.create.enable=true
            var cluster = initWithRemoteDefaultEnabled();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // Given a classic topic
                String topicName = "classicTopic";
                createTopic(admin, topicName, Map.of());
                var topicConfig = getTopicConfig(admin, topicName);
                assertEquals("true", topicConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", topicConfig.get(DISKLESS_ENABLE_CONFIG));

                // When remote storage is disabled
                var disableRemoteStorageError = incrementalAlterTopicConfig(admin, topicName,
                    Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"));
                // Then an error is thrown
                assertEquals("It is invalid to disable remote storage without deleting remote data. " +
                    "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
                    "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.", disableRemoteStorageError.get());

                // When diskless is enabled
                var enableDisklessError =
                    incrementalAlterTopicConfig(admin, topicName, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
                // Then an error is thrown
                assertEquals("It is invalid to enable diskless on an already existing topic.", enableDisklessError.get());

            } finally {
                cluster.close();
            }
        }

        @Test
        void alteringConfigOfDisklessTopicReturnsError() throws Exception {
            // Given a cluster with default.remote.storage.for.topic.create.enable=true
            var cluster = initWithRemoteDefaultEnabled();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // Given a diskless topic
                String topicName = "disklessTopic";
                createTopic(admin, topicName, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
                var topicConfig = getTopicConfig(admin, topicName);
                assertEquals("true", topicConfig.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", topicConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // When diskless is disabled
                var disableDisklessError = incrementalAlterTopicConfig(admin, topicName, Map.of(DISKLESS_ENABLE_CONFIG, "false"));
                // Then an error is thrown
                assertEquals("It is invalid to disable diskless.", disableDisklessError.get());

                // When remote storage is enabled
                var enableRemoteStorageError = incrementalAlterTopicConfig(admin, topicName,
                    Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"));
                // Then an error is thrown
                assertEquals("It is invalid to enable remote storage on an existing diskless topic.", enableRemoteStorageError.get());

            } finally {
                cluster.close();
            }
        }

        @Test
        void deletingConfigsOfClassicTopicReturnsError() throws Exception {
            // Given a cluster with default.remote.storage.for.topic.create.enable=true
            var cluster = initWithRemoteDefaultEnabled();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // Given a classic topic
                String topicName = "classicTopic";
                createTopic(admin, topicName, Map.of());
                var topicConfig = getTopicConfig(admin,  topicName);
                assertEquals("true", topicConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", topicConfig.get(DISKLESS_ENABLE_CONFIG));

                // When remote storage config is deleted
                var deleteRemoteStorageError = deleteTopicConfigs(admin, topicName, List.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                // Then an error is thrown
                assertEquals("It is invalid to disable remote storage without deleting remote data. " +
                    "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
                    "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.", deleteRemoteStorageError.get());

                // When diskless config is deleted
                var deleteDisklessError = deleteTopicConfigs(admin, topicName, List.of(DISKLESS_ENABLE_CONFIG));
                // Then an error is thrown
                assertEquals("It is not allowed to delete the diskless.enable config", deleteDisklessError.get());

            } finally {
                cluster.close();
            }
        }

        @Test
        void deletingConfigsOfDisklessTopicReturnsError() throws Exception {
            // Given a cluster with default.remote.storage.for.topic.create.enable=true
            var cluster = initWithRemoteDefaultEnabled();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // Given a diskless topic
                String topicName = "disklessTopic";
                createTopic(admin, topicName, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
                var topicConfig = getTopicConfig(admin,  topicName);
                assertEquals("true", topicConfig.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", topicConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // When diskless is deleted
                var deleteDisklessError = deleteTopicConfigs(admin, topicName, List.of(DISKLESS_ENABLE_CONFIG));
                // Then an error is thrown
                assertEquals("It is not allowed to delete the diskless.enable config", deleteDisklessError.get());

                // When remote storage config is deleted
                var deleteRemoteStorageError = deleteTopicConfigs(admin, topicName, List.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                // Then no error is thrown
                assertTrue(deleteRemoteStorageError.isEmpty());

            } finally {
                cluster.close();
            }
        }

        private Optional<String> incrementalAlterTopicConfig(Admin admin, String topic, Map<String, String> newConfigs) {
            var topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            var operations = newConfigs.entrySet().stream()
                .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
                .toList();
            try {
                admin.incrementalAlterConfigs(Map.of(topicResource, operations)).all().get(10, TimeUnit.SECONDS);
                return Optional.empty();
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                return Optional.ofNullable(message);
            }
        }

        private Optional<String> deleteTopicConfigs(Admin admin, String topic, Collection<String> configsToDelete) throws Exception {
            var topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            var deleteEntries = configsToDelete.stream().map(
                    configToDelete -> new AlterConfigOp(new ConfigEntry(configToDelete, ""), AlterConfigOp.OpType.DELETE))
                .toList();
            try {
                admin.incrementalAlterConfigs(Map.of(topicResource, deleteEntries)).all().get(10, TimeUnit.SECONDS);
                return Optional.empty();
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                return Optional.ofNullable(message);
            }
        }

    }

    @Nested
    class LegacyAlterConfigs {
        @Test
        void alteringConfigOfClassicTopicReturnsError() throws Exception {
            // Given a cluster with default.remote.storage.for.topic.create.enable=true
            var cluster = initWithRemoteDefaultEnabled();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // Given a classic topic
                String topicName = "classicTopic";
                createTopic(admin, topicName, Map.of());
                var topicConfig = getTopicConfig(admin, topicName);
                assertEquals("true", topicConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
                assertEquals("false", topicConfig.get(DISKLESS_ENABLE_CONFIG));

                // When remote storage is disabled
                var disableRemoteStorageDisableDisklessConfigs = Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false", DISKLESS_ENABLE_CONFIG, "false");
                var disableRemoteStorageDisableDisklessError = legacyAlterTopicConfig(cluster.bootstrapServers(), topicName, disableRemoteStorageDisableDisklessConfigs);
                // Then an error is thrown
                assertEquals("It is invalid to disable remote storage without deleting remote data. " +
                    "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
                    "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.", disableRemoteStorageDisableDisklessError.get());

                // When remote storage is disabled and diskless is not set
                var disableRemoteStorageConfigs = Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
                var disableRemoteStorageError = legacyAlterTopicConfig(cluster.bootstrapServers(), topicName, disableRemoteStorageConfigs);
                // Then an error is thrown
                assertEquals("It is invalid to disable remote storage without deleting remote data. " +
                    "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
                    "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.", disableRemoteStorageError.get());

                // When diskless is enabled
                var enableDisklessEnableRemoteStorageConfigs = Map.of(DISKLESS_ENABLE_CONFIG, "true",  REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
                var enableDisklessEnableRemoteStorageError = legacyAlterTopicConfig(cluster.bootstrapServers(), topicName, enableDisklessEnableRemoteStorageConfigs);
                // Then an error is thrown
                assertEquals("It is invalid to enable diskless on an already existing topic.", enableDisklessEnableRemoteStorageError.get());

                // When diskless is enabled and remote storage is not set
                var enableDisklessConfigs = Map.of(DISKLESS_ENABLE_CONFIG, "true");
                var enableDisklessError = legacyAlterTopicConfig(cluster.bootstrapServers(), topicName, enableDisklessConfigs);
                // Then an error is thrown
                assertEquals("It is invalid to disable remote storage without deleting remote data. " +
                    "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
                    "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.", enableDisklessError.get());

            } finally {
                cluster.close();
            }
        }

        @Test
        void alteringConfigOfDisklessTopicReturnsError() throws Exception {
            // Given a cluster with default.remote.storage.for.topic.create.enable=true
            var cluster = initWithRemoteDefaultEnabled();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                // Given a diskless topic
                String topicName = "disklessTopic";
                createTopic(admin, topicName, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
                var topicConfig = getTopicConfig(admin, topicName);
                assertEquals("true", topicConfig.get(DISKLESS_ENABLE_CONFIG));
                assertEquals("false", topicConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // When diskless is disabled
                var disableDisklessDisableRemoteStorageConfigs = Map.of(DISKLESS_ENABLE_CONFIG, "false",  REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
                var disableDisklessDisableRemoteStorageError = legacyAlterTopicConfig(cluster.bootstrapServers(), topicName, disableDisklessDisableRemoteStorageConfigs);
                // Then an error is thrown
                assertEquals("It is invalid to disable diskless.", disableDisklessDisableRemoteStorageError.get());

                // When diskless is disabled and remote storage is not set
                var disableDisklessConfigs = Map.of(DISKLESS_ENABLE_CONFIG, "false");
                var disableDisklessError = legacyAlterTopicConfig(cluster.bootstrapServers(), topicName, disableDisklessConfigs);
                // Then an error is thrown
                assertEquals("It is invalid to disable diskless.", disableDisklessError.get());

                // When remote storage is enabled
                var enableRemoteStorageEnableDisklessConfigs = Map.of(DISKLESS_ENABLE_CONFIG, "true",  REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
                var enableRemoteStorageEnableDisklessError = legacyAlterTopicConfig(cluster.bootstrapServers(), topicName, enableRemoteStorageEnableDisklessConfigs);
                // Then an error is thrown
                assertEquals("It is invalid to enable remote storage on an existing diskless topic.", enableRemoteStorageEnableDisklessError.get());

                // When remote storage is enabled and diskless is not set
                var enableRemoteStorageConfigs = Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
                var enableRemoteStorageError = legacyAlterTopicConfig(cluster.bootstrapServers(), topicName, enableRemoteStorageConfigs);
                // Then an error is thrown
                assertEquals("It is invalid to disable diskless.", enableRemoteStorageError.get());
            } finally {
                cluster.close();
            }
        }

        private Optional<String> legacyAlterTopicConfig(String bootstrapServers, String topic, Map<String, String> newConfigs) throws Exception {
            var topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            List<AlterConfigsRequest.ConfigEntry> entries = newConfigs.entrySet().stream()
                .map(entry -> new AlterConfigsRequest.ConfigEntry(entry.getKey(), entry.getValue()))
                .toList();
            var request = new AlterConfigsRequest.Builder(
                Map.of(topicResource, new AlterConfigsRequest.Config(entries)),
                false
            ).build(ApiKeys.ALTER_CONFIGS.latestVersion());

            try (Socket socket = connect(bootstrapServers)) {
                send(request, socket);
                AlterConfigsResponse response = receiveAlterConfigsResponse(socket, request.version());
                ApiError error = response.errors().get(topicResource);
                if (error == null || error.error() == Errors.NONE) {
                    return Optional.empty();
                }
                return Optional.ofNullable(error.message());
            }
        }

        private Socket connect(String bootstrapServers) throws Exception {
            String[] hostPort = bootstrapServers.split(",", 2)[0].split(":", 2);
            return new Socket(hostPort[0], Integer.parseInt(hostPort[1]));
        }

        private void send(AlterConfigsRequest request, Socket socket) throws Exception {
            var header = new RequestHeader(ApiKeys.ALTER_CONFIGS, request.version(), "client-id", 1);
            var outgoing = new DataOutputStream(socket.getOutputStream());
            byte[] serializedBytes = Utils.toArray(request.serializeWithHeader(header));
            outgoing.writeInt(serializedBytes.length);
            outgoing.write(serializedBytes);
            outgoing.flush();
        }

        private AlterConfigsResponse receiveAlterConfigsResponse(Socket socket, short version) throws Exception {
            var incoming = new DataInputStream(socket.getInputStream());
            int len = incoming.readInt();
            byte[] responseBytes = new byte[len];
            incoming.readFully(responseBytes);

            ByteBuffer responseBuffer = ByteBuffer.wrap(responseBytes);
            ResponseHeader.parse(responseBuffer, ApiKeys.ALTER_CONFIGS.responseHeaderVersion(version));
            AbstractResponse response = AbstractResponse.parseResponse(
                ApiKeys.ALTER_CONFIGS,
                new ByteBufferAccessor(responseBuffer),
                version
            );
            return (AlterConfigsResponse) response;
        }
    }

    private KafkaClusterTestKit initWithRemoteDefaultEnabled() throws Exception {
        return initWithRemoteDefaultEnabled("");
    }

    private KafkaClusterTestKit initWithRemoteDefaultEnabled(String localOnlyTopicRegex) throws Exception {
        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .build();
        var cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerLogConfigs.DISKLESS_ENABLE_CONFIG, "false")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, "false")
            .setConfigProp(ServerConfigs.DEFAULT_REMOTE_STORAGE_FOR_TOPIC_CREATE_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DEFAULT_REMOTE_STORAGE_FOR_TOPIC_CREATE_LOCAL_ONLY_TOPIC_REGEX_CONFIG,
                localOnlyTopicRegex)
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG, pgContainer.getJdbcUrl())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_BACKEND_CLASS_CONFIG, S3Storage.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_BUCKET_NAME_CONFIG, s3Container.getBucketName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_REGION_CONFIG, s3Container.getRegion())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_ENDPOINT_URL_CONFIG, s3Container.getEndpoint())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG, "true")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, s3Container.getAccessKey())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey())
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        return cluster;
    }

    private Map<String, String> createTopic(Admin admin, String topic, Map<String, String> configs) throws Exception {
        var createTopicsResult = admin.createTopics(Collections.singletonList(
            new NewTopic(topic, 1, (short) 1).configs(configs)
        ));
        var createdTopicConfig = createTopicsResult.config(topic).get(10, TimeUnit.SECONDS);
        return createdTopicConfig.entries().stream().collect(
            HashMap::new,
            (map, entry) -> map.put(entry.name(), entry.value()),
            HashMap::putAll
        );
    }

    private Map<String, String> getTopicConfig(Admin admin, String topic)
        throws ExecutionException, InterruptedException, TimeoutException {
        int maxRetries = 3;
        long retryDelayMs = 1000;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                var topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                var describeConfigsResult = admin.describeConfigs(Collections.singletonList(topicResource));
                var allConfigs = describeConfigsResult.all().get(10, TimeUnit.SECONDS);
                return allConfigs.get(topicResource).entries().stream().collect(
                    HashMap::new,
                    (map, entry) -> map.put(entry.name(), entry.value()),
                    HashMap::putAll
                );
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
}
