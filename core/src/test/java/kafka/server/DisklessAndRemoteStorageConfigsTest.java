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
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
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

import java.util.Collections;
import java.util.HashMap;
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
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class DisklessAndRemoteStorageConfigsTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final String ENABLE_DISKLESS_ERROR = "It is invalid to enable diskless on an already existing topic.";
    private static final String DISABLE_DISKLESS_ERROR = "It is invalid to disable diskless.";
    private static final String DISKLESS_REMOTE_SET_ERROR = "It is not valid to set a value for both diskless.enable and remote.storage.enable.";
    private static final String DISABLE_REMOTE_WITHOUT_DELETE_ERROR = "It is invalid to disable remote storage without deleting remote data. "
        + "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. "
        + "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.";

    @BeforeEach
    public void setup(final TestInfo testInfo) {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
    }

    @Nested
    class CreateTopic {
        @Test
        void validatesCreationCases() throws Exception {
            var cluster = initCluster();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                createTopicAndAssertEffective(admin, "no-diskless-no-remote", Map.of(), "false", "false");
                createTopicAndAssertEffective(admin, "diskless-true", Map.of(DISKLESS_ENABLE_CONFIG, "true"), "true", "false");
                createTopicAndAssertEffective(admin, "remote-true", Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), "false", "true");
                final Optional<String> disklessFalseRemoteFalseError = createTopic(admin, "diskless-false-remote-false-invalid", Map.of(
                    DISKLESS_ENABLE_CONFIG, "false",
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"
                ));
                assertEquals(DISKLESS_REMOTE_SET_ERROR, disklessFalseRemoteFalseError.get());

                final Optional<String> disklessFalseRemoteTrueError = createTopic(admin, "diskless-false-remote-true-invalid", Map.of(
                    DISKLESS_ENABLE_CONFIG, "false",
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
                ));
                assertEquals(DISKLESS_REMOTE_SET_ERROR, disklessFalseRemoteTrueError.get());

                final Optional<String> disklessTrueRemoteFalseError = createTopic(admin, "diskless-true-remote-false-invalid", Map.of(
                    DISKLESS_ENABLE_CONFIG, "true",
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"
                ));
                assertEquals(DISKLESS_REMOTE_SET_ERROR, disklessTrueRemoteFalseError.get());

                final Optional<String> disklessTrueRemoteTrueError = createTopic(admin, "diskless-true-remote-true-invalid", Map.of(
                    DISKLESS_ENABLE_CONFIG, "true",
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
                ));
                assertEquals(DISKLESS_REMOTE_SET_ERROR, disklessTrueRemoteTrueError.get());
            } finally {
                cluster.close();
            }
        }
    }

    @Nested
    class IncrementalAlterConfigs {
        @Test
        void validatesUpdateCases() throws Exception {
            var cluster = initCluster();
            Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin admin = AdminClient.create(clientConfigs)) {
                String setDisklessTrueFromEmptyConfigsTopic = "set-diskless-true-from-empty-configs";
                createTopicAndAssertEffective(admin, setDisklessTrueFromEmptyConfigsTopic, Map.of(), "false", "false");
                assertEquals(ENABLE_DISKLESS_ERROR, incrementalAlterTopicConfig(admin, setDisklessTrueFromEmptyConfigsTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true")).get());

                String setDisklessTrueFromDisklessFalseTopic = "set-diskless-true-from-diskless-false";
                createTopicAndAssertEffective(admin, setDisklessTrueFromDisklessFalseTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false"), "false", "false");
                assertEquals(ENABLE_DISKLESS_ERROR, incrementalAlterTopicConfig(admin, setDisklessTrueFromDisklessFalseTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true")).get());

                String keepDisklessTrueTopic = "keep-diskless-true";
                createTopicAndAssertEffective(admin, keepDisklessTrueTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true"), "true", "false");
                assertTrue(incrementalAlterTopicConfig(admin, keepDisklessTrueTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true")).isEmpty());

                String setDisklessTrueFromRemoteFalseTopic = "set-diskless-true-from-remote-false";
                createTopicAndAssertEffective(admin, setDisklessTrueFromRemoteFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"), "false", "false");
                assertEquals(ENABLE_DISKLESS_ERROR, incrementalAlterTopicConfig(admin, setDisklessTrueFromRemoteFalseTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true")).get());

                String setDisklessTrueFromRemoteTrueTopic = "set-diskless-true-from-remote-true";
                createTopicAndAssertEffective(admin, setDisklessTrueFromRemoteTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), "false", "true");
                assertEquals(ENABLE_DISKLESS_ERROR, incrementalAlterTopicConfig(admin, setDisklessTrueFromRemoteTrueTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true")).get());

                String setDisklessFalseFromEmptyConfigsTopic = "set-diskless-false-from-empty-configs";
                createTopicAndAssertEffective(admin, setDisklessFalseFromEmptyConfigsTopic, Map.of(), "false", "false");
                assertTrue(incrementalAlterTopicConfig(admin, setDisklessFalseFromEmptyConfigsTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false")).isEmpty());

                String keepDisklessFalseTopic = "keep-diskless-false";
                createTopicAndAssertEffective(admin, keepDisklessFalseTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false"), "false", "false");
                assertTrue(incrementalAlterTopicConfig(admin, keepDisklessFalseTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false")).isEmpty());

                String setDisklessFalseFromDisklessTrueTopic = "set-diskless-false-from-diskless-true";
                createTopicAndAssertEffective(admin, setDisklessFalseFromDisklessTrueTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true"), "true", "false");
                assertEquals(DISABLE_DISKLESS_ERROR, incrementalAlterTopicConfig(admin, setDisklessFalseFromDisklessTrueTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false")).get());

                String setDisklessFalseFromRemoteFalseTopic = "set-diskless-false-from-remote-false";
                createTopicAndAssertEffective(admin, setDisklessFalseFromRemoteFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"), "false", "false");
                assertEquals(DISKLESS_REMOTE_SET_ERROR, incrementalAlterTopicConfig(admin, setDisklessFalseFromRemoteFalseTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false")).get());

                String setDisklessFalseFromRemoteTrueTopic = "set-diskless-false-from-remote-true";
                createTopicAndAssertEffective(admin, setDisklessFalseFromRemoteTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), "false", "true");
                assertEquals(DISKLESS_REMOTE_SET_ERROR, incrementalAlterTopicConfig(admin, setDisklessFalseFromRemoteTrueTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false")).get());

                String setRemoteTrueFromEmptyConfigsTopic = "set-remote-true-from-empty-configs";
                createTopicAndAssertEffective(admin, setRemoteTrueFromEmptyConfigsTopic, Map.of(), "false", "false");
                assertTrue(incrementalAlterTopicConfig(admin, setRemoteTrueFromEmptyConfigsTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")).isEmpty());

                String setRemoteTrueFromDisklessFalseTopic = "set-remote-true-from-diskless-false";
                createTopicAndAssertEffective(admin, setRemoteTrueFromDisklessFalseTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false"), "false", "false");
                assertEquals(DISKLESS_REMOTE_SET_ERROR, incrementalAlterTopicConfig(admin, setRemoteTrueFromDisklessFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")).get());

                String setRemoteTrueFromDisklessTrueTopic = "set-remote-true-from-diskless-true";
                createTopicAndAssertEffective(admin, setRemoteTrueFromDisklessTrueTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true"), "true", "false");
                assertEquals(DISKLESS_REMOTE_SET_ERROR, incrementalAlterTopicConfig(admin, setRemoteTrueFromDisklessTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")).get());

                String setRemoteTrueFromRemoteFalseTopic = "set-remote-true-from-remote-false";
                createTopicAndAssertEffective(admin, setRemoteTrueFromRemoteFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"), "false", "false");
                assertTrue(incrementalAlterTopicConfig(admin, setRemoteTrueFromRemoteFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")).isEmpty());

                String keepRemoteTrueTopic = "keep-remote-true";
                createTopicAndAssertEffective(admin, keepRemoteTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), "false", "true");
                assertTrue(incrementalAlterTopicConfig(admin, keepRemoteTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")).isEmpty());

                String setRemoteFalseFromEmptyConfigsTopic = "set-remote-false-from-empty-configs";
                createTopicAndAssertEffective(admin, setRemoteFalseFromEmptyConfigsTopic, Map.of(), "false", "false");
                assertTrue(incrementalAlterTopicConfig(admin, setRemoteFalseFromEmptyConfigsTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")).isEmpty());

                String setRemoteFalseFromDisklessFalseTopic = "set-remote-false-from-diskless-false";
                createTopicAndAssertEffective(admin, setRemoteFalseFromDisklessFalseTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false"), "false", "false");
                assertEquals(DISKLESS_REMOTE_SET_ERROR, incrementalAlterTopicConfig(admin, setRemoteFalseFromDisklessFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")).get());

                String setRemoteFalseFromDisklessTrueTopic = "set-remote-false-from-diskless-true";
                createTopicAndAssertEffective(admin, setRemoteFalseFromDisklessTrueTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true"), "true", "false");
                assertEquals(DISKLESS_REMOTE_SET_ERROR, incrementalAlterTopicConfig(admin, setRemoteFalseFromDisklessTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")).get());

                String keepRemoteFalseTopic = "keep-remote-false";
                createTopicAndAssertEffective(admin, keepRemoteFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"), "false", "false");
                assertTrue(incrementalAlterTopicConfig(admin, keepRemoteFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")).isEmpty());

                String setRemoteFalseFromRemoteTrueTopic = "set-remote-false-from-remote-true";
                createTopicAndAssertEffective(admin, setRemoteFalseFromRemoteTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), "false", "true");
                assertEquals(DISABLE_REMOTE_WITHOUT_DELETE_ERROR, incrementalAlterTopicConfig(admin, setRemoteFalseFromRemoteTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")).get());

                String disableRemoteWithDeleteOnDisableTopic = "disable-remote-with-delete-on-disable";
                createTopicAndAssertEffective(admin, disableRemoteWithDeleteOnDisableTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), "false", "true");
                assertTrue(incrementalAlterTopicConfig(admin, disableRemoteWithDeleteOnDisableTopic, Map.of(
                    REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false",
                    REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, "true"
                )).isEmpty());
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
    }

    private KafkaClusterTestKit initCluster() throws Exception {
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

    private Optional<String> createTopic(Admin admin, String topic, Map<String, String> configs) {
        try {
            admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1).configs(configs)))
                .all()
                .get(10, TimeUnit.SECONDS);
            return Optional.empty();
        } catch (Exception e) {
            String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            return Optional.ofNullable(message);
        }
    }

    private void createTopicAndAssertEffective(Admin admin,
                                               String topic,
                                               Map<String, String> configs,
                                               String expectedDiskless,
                                               String expectedRemoteStorage) throws Exception {
        assertTrue(createTopic(admin, topic, configs).isEmpty());
        var topicConfig = getTopicConfig(admin, topic);
        assertEquals(expectedDiskless, topicConfig.get(DISKLESS_ENABLE_CONFIG));
        assertEquals(expectedRemoteStorage, topicConfig.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
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
