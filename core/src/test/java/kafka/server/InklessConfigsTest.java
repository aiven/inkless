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
import org.apache.kafka.clients.admin.CreateTopicsResult;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.DISKLESS_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
public class InklessConfigsTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private KafkaClusterTestKit init(boolean defaultDisklessEnableConfig, boolean disklessStorageEnableConfig) throws Exception {
        return init(defaultDisklessEnableConfig, disklessStorageEnableConfig, false);
    }

    private KafkaClusterTestKit init(boolean defaultDisklessEnableConfig, boolean disklessStorageEnableConfig, boolean isDisklessAllowFromClassicEnabled) throws Exception {
        return init(defaultDisklessEnableConfig, disklessStorageEnableConfig, isDisklessAllowFromClassicEnabled, false, List.of());
    }

    private KafkaClusterTestKit init(boolean defaultDisklessEnableConfig,
                                     boolean disklessStorageEnableConfig,
                                     boolean isDisklessAllowFromClassicEnabled,
                                     boolean classicRemoteStorageForceEnabled,
                                     List<String> classicRemoteStorageForceExcludeTopicRegexes) throws Exception {
        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .build();
        var cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerLogConfigs.DISKLESS_ENABLE_CONFIG, String.valueOf(defaultDisklessEnableConfig))
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, String.valueOf(disklessStorageEnableConfig))
            .setConfigProp(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, String.valueOf(isDisklessAllowFromClassicEnabled))
            .setConfigProp(ServerConfigs.CLASSIC_REMOTE_STORAGE_FORCE_ENABLE_CONFIG, String.valueOf(classicRemoteStorageForceEnabled))
            .setConfigProp(ServerConfigs.CLASSIC_REMOTE_STORAGE_FORCE_EXCLUDE_TOPIC_REGEXES_CONFIG,
                String.join(",", classicRemoteStorageForceExcludeTopicRegexes))
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
            // PG control plane config
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG, pgContainer.getJdbcUrl())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD)
            // S3 storage config
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

    @BeforeEach
    public void setup(final TestInfo testInfo) {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void disklessTopicConfigs(boolean defaultDisklessEnableConfig) throws Exception {
        var cluster = init(defaultDisklessEnableConfig, true);
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        Admin admin = AdminClient.create(clientConfigs);

        // When creating a new topic with diskless.enable=true
        final String disklessTopic = "disklessTopic";
        createTopic(admin, disklessTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
        // Then diskless.enable is set to true in the topic config
        var disklessTopicConfig = getTopicConfig(admin, disklessTopic);
        assertEquals("true", disklessTopicConfig.get(DISKLESS_ENABLE_CONFIG));
        // Then it's not possible turn off diskless after the topic is created
        assertThrows(ExecutionException.class, () -> alterTopicConfig(admin, disklessTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false")));
        // Then it's not possible to delete the diskless.enable config
        assertThrows(ExecutionException.class, () -> deleteTopicConfigs(admin, disklessTopic, List.of(DISKLESS_ENABLE_CONFIG)));

        admin.close();
        cluster.close();
    }


    @Test
    public void classicTopicWithDisklessDefaultFalseConfigs() throws Exception {
        var cluster = init(false, true);
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        Admin admin = AdminClient.create(clientConfigs);

        // When creating a new topic without specifying diskless.enable
        final String classicTopic = "classicTopic";
        createTopic(admin, classicTopic, Map.of());
        // Then diskless.enable is set to false in the topic config
        var classicTopicConfig = getTopicConfig(admin, classicTopic);
        assertEquals("false", classicTopicConfig.get(DISKLESS_ENABLE_CONFIG));
        // Then it's not possible turn on diskless after the topic is created
        assertThrows(ExecutionException.class, () -> alterTopicConfig(admin, classicTopic, Map.of(DISKLESS_ENABLE_CONFIG, "true")));
        // Then it's not possible to delete the diskless.enable config
        assertThrows(ExecutionException.class, () -> deleteTopicConfigs(admin, classicTopic, List.of(DISKLESS_ENABLE_CONFIG)));

        // When creating a new topic with diskless.enable=false
        final String disklessDisabledTopic = "disklessDisabledTopic";
        createTopic(admin, disklessDisabledTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false"));
        // Then diskless.enable is set to false in the topic config
        var disklessDisabledTopicConfig = getTopicConfig(admin, disklessDisabledTopic);
        assertEquals("false", disklessDisabledTopicConfig.get(DISKLESS_ENABLE_CONFIG));
        // Then it's not possible turn on diskless after the topic is created
        assertThrows(ExecutionException.class, () -> alterTopicConfig(admin, disklessDisabledTopic, Map.of(
            DISKLESS_ENABLE_CONFIG, "true")));
        // Then it's not possible to delete the diskless.enable config
        assertThrows(ExecutionException.class, () -> deleteTopicConfigs(admin, classicTopic, List.of(DISKLESS_ENABLE_CONFIG)));

        admin.close();
        cluster.close();
    }

    @Test
    public void classicTopicWithDisklessDefaultTrueConfigs() throws Exception {
        var cluster = init(true, true);
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        try (Admin admin = AdminClient.create(clientConfigs)) {
            // When creating a new topic with diskless.enable=false
            final String disklessDisabledTopic = "disklessDisabledTopic";
            createTopic(admin, disklessDisabledTopic, Map.of(DISKLESS_ENABLE_CONFIG, "false"));
            // Then diskless.enable is set to false in the topic config
            var disklessDisabledTopicConfig = getTopicConfig(admin, disklessDisabledTopic);
            assertEquals("false", disklessDisabledTopicConfig.get(DISKLESS_ENABLE_CONFIG));
            // Then it's not possible turn on diskless after the topic is created
            assertThrows(ExecutionException.class, () -> alterTopicConfig(admin, disklessDisabledTopic, Map.of(
                DISKLESS_ENABLE_CONFIG, "true")));
            // Then it's not possible to delete diskless.enable=false because the default is true and it would enable diskless
            assertThrows(ExecutionException.class, () -> deleteTopicConfigs(admin, disklessDisabledTopic, List.of(DISKLESS_ENABLE_CONFIG)));
        }
        cluster.close();
    }

    @Nested
    final class ClassicRemoteStorageForcePolicy {
        private static final List<String> EXCLUDED_TOPIC_REGEXES = List.of("_schemas", "mm2-(.*)");

        private KafkaClusterTestKit initWithClassicRemoteStorageForceEnabled() throws Exception {
            return init(false, true, false, true, EXCLUDED_TOPIC_REGEXES);
        }

        @Test
        void remoteStorageEnableIsAlwaysTrueForClassicTopics() throws Exception {
            final KafkaClusterTestKit cluster = initWithClassicRemoteStorageForceEnabled();
            final Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (final Admin admin = AdminClient.create(clientConfigs)) {
                final String noRemoteConfigTopic = "classic-no-remote-config";
                assertEquals("true", createTopicAndGetRemoteStorageFromCreateResponse(admin, noRemoteConfigTopic, Map.of()));
                assertEquals("true", getTopicConfig(admin, noRemoteConfigTopic).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                final String remoteFalseTopic = "classic-remote-false";
                assertEquals("true", createTopicAndGetRemoteStorageFromCreateResponse(
                    admin, remoteFalseTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")));
                assertEquals("true", getTopicConfig(admin, remoteFalseTopic).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                final String remoteTrueTopic = "classic-remote-true";
                assertEquals("true", createTopicAndGetRemoteStorageFromCreateResponse(
                    admin, remoteTrueTopic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")));
                assertEquals("true", getTopicConfig(admin, remoteTrueTopic).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
            } finally {
                cluster.close();
            }
        }

        @Test
        void compactedTopicsAreExcludedFromForcePolicy() throws Exception {
            final KafkaClusterTestKit cluster = initWithClassicRemoteStorageForceEnabled();
            final Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (final Admin admin = AdminClient.create(clientConfigs)) {
                // Compacted topic gets remote.storage.enable=false when not specified
                final String compactedNoRemoteTopic = "compacted-no-remote";
                assertEquals("false", createTopicAndGetRemoteStorageFromCreateResponse(
                    admin, compactedNoRemoteTopic, Map.of(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT)));
                assertEquals("false", getTopicConfig(admin, compactedNoRemoteTopic).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // Setting remote.storage.enable=false is allowed for a compacted topic
                final String compactedRemoteFalseTopic = "compacted-remote-false";
                assertEquals("false", createTopicAndGetRemoteStorageFromCreateResponse(
                    admin,
                    compactedRemoteFalseTopic,
                    Map.of(
                        CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT,
                        REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"
                    )));
                assertEquals("false", getTopicConfig(admin, compactedRemoteFalseTopic).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // Compacted tiered topics are not supported by Kafka
                final String compactedRemoteTrueTopic = "compacted-remote-true";
                final ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> createTopicAndGetRemoteStorageFromCreateResponse(
                        admin,
                        compactedRemoteTrueTopic,
                        Map.of(
                            CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT,
                            REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
                        )
                    )
                );
                assertEquals(
                    "Remote log storage is unsupported for the compacted topics",
                    exception.getCause().getMessage()
                );
            } finally {
                cluster.close();
            }
        }

        @Test
        void regexExcludedTopicsAreExcludedFromForcePolicy() throws Exception {
            final KafkaClusterTestKit cluster = initWithClassicRemoteStorageForceEnabled();
            final Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (final Admin admin = AdminClient.create(clientConfigs)) {
                // Excluded topic gets remote.storage.enable=false when not setting it
                final String schemasTopic = "_schemas";
                assertEquals("false", createTopicAndGetRemoteStorageFromCreateResponse(admin, schemasTopic, Map.of()));
                assertEquals("false", getTopicConfig(admin, schemasTopic).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // Setting remote.storage.enable=false is allowed for an excluded topic
                final String mm2TopicRemoteFalse = "mm2-heartbeats";
                assertEquals("false", createTopicAndGetRemoteStorageFromCreateResponse(
                    admin, mm2TopicRemoteFalse, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")));
                assertEquals("false", getTopicConfig(admin, mm2TopicRemoteFalse).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));

                // Excluded topic can still have remote.storage.enable=true if explicitly set
                final String mm2TopicRemoteTrue = "mm2-checkpoints";
                assertEquals("true", createTopicAndGetRemoteStorageFromCreateResponse(
                    admin, mm2TopicRemoteTrue, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")));
                assertEquals("true", getTopicConfig(admin, mm2TopicRemoteTrue).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
            } finally {
                cluster.close();
            }
        }

        @Test
        void compactedRegexExcludedTopicIsExcludedFromForcePolicy() throws Exception {
            final KafkaClusterTestKit cluster = initWithClassicRemoteStorageForceEnabled();
            final Map<String, Object> clientConfigs = new HashMap<>();
            clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (final Admin admin = AdminClient.create(clientConfigs)) {
                final String compactedSchemasTopic = "_schemas";
                assertEquals("false", createTopicAndGetRemoteStorageFromCreateResponse(
                    admin,
                    compactedSchemasTopic,
                    Map.of(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT)
                ));
                assertEquals("false", getTopicConfig(admin, compactedSchemasTopic).get(REMOTE_LOG_STORAGE_ENABLE_CONFIG));
            } finally {
                cluster.close();
            }
        }
    }

    public void createTopic(Admin admin, String topic, Map<String, String> configs) throws Exception {
        admin.createTopics(Collections.singletonList(
            new NewTopic(topic, 1, (short) 1)
                .configs(configs)
        )).all().get(10, TimeUnit.SECONDS);
    }

    private String createTopicAndGetRemoteStorageFromCreateResponse(
        final Admin admin,
        final String topic,
        final Map<String, String> configs
    ) throws Exception {
        final CreateTopicsResult createResult = admin.createTopics(Collections.singletonList(
            new NewTopic(topic, 1, (short) 1).configs(configs)
        ));
        createResult.all().get(10, TimeUnit.SECONDS);
        final ConfigEntry remoteStorageConfig = createResult.config(topic).get(10, TimeUnit.SECONDS)
            .get(REMOTE_LOG_STORAGE_ENABLE_CONFIG);
        assertNotNull(remoteStorageConfig);
        return remoteStorageConfig.value();
    }

    private Map<String, String> getTopicConfig(Admin admin, String topic)
        throws ExecutionException, InterruptedException, TimeoutException {
        int maxRetries = 3;
        long retryDelayMs = 1000; // 1 second

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                var topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                var describeConfigsResult = admin.describeConfigs(Collections.singletonList(topicResource));
                var allConfigs = describeConfigsResult.all().get(10, TimeUnit.SECONDS);

                return allConfigs
                    .get(topicResource).entries().stream()
                    .collect(
                        HashMap::new,
                        (map, entry) -> map.put(entry.name(), entry.value()),
                        HashMap::putAll
                    );
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    System.err.println(
                        "Attempt " + attempt + " failed: " + e.getCause().getMessage() + ". Retrying..."
                    );
                    if (attempt == maxRetries) {
                        throw e;
                    }
                    Thread.sleep(retryDelayMs);
                } else {
                    throw e;
                }
            }
        }
        throw new IllegalStateException("Exited retry loop unexpectedly.");
    }

    private void alterTopicConfig(Admin admin, String topic, Map<String, String> newConfigs) throws Exception {
        var topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        var operations = newConfigs.entrySet().stream()
            .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
            .toList();
        admin.incrementalAlterConfigs(Map.of(topicResource, operations)).all().get(10, TimeUnit.SECONDS);
    }

    private void deleteTopicConfigs(Admin admin, String topic, Collection<String> configsToDelete) throws Exception {
        var topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        var deleteEntries = configsToDelete.stream().map(configToDelete -> new AlterConfigOp(new ConfigEntry(configToDelete, ""), AlterConfigOp.OpType.DELETE)).toList();
        admin.incrementalAlterConfigs(Map.of(topicResource, deleteEntries)).all().get(10, TimeUnit.SECONDS);
    }

}
