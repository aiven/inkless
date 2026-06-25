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
package org.apache.kafka.server.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.util.MirrorFilterUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * This class provides proper validation, defaults, and documentation for all
 * configurations supported by the Cluster Mirroring feature.
 */
public final class ClusterMirrorConfig {
    // ---------------------------------------------------------------
    // Broker level configs: stored in server.properties or dynamic broker config.
    // ---------------------------------------------------------------

    public static final String MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG = "mirror.state.topic.num.partitions";
    public static final int MIRROR_STATE_TOPIC_NUM_PARTITIONS_DEFAULT = 50;
    public static final String MIRROR_STATE_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions for the internal topic (should not change after deployment).";

    public static final String MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG = "mirror.state.topic.replication.factor";
    public static final short MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the internal topic. " +
            "Topic creation will fail until the cluster size meets this replication factor requirement.";

    public static final String MIRROR_NUM_REPLICA_FETCHERS_CONFIG = "mirror.num.replica.fetchers";
    public static final int MIRROR_NUM_REPLICA_FETCHERS_DEFAULT = 1;
    public static final String MIRROR_NUM_REPLICA_FETCHERS_DOC = "Number of fetcher threads used to replicate records from each source broker in a cluster mirror. " +
            "The total number of mirror fetcher threads on a broker equals this value multiplied by the number of distinct source brokers and the number of cluster mirrors. " +
            "A higher value increases I/O parallelism for cross cluster replication at the cost of higher CPU and memory utilization.";

    public static final String MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG = "mirror.metadata.refresh.interval.ms";
    public static final long MIRROR_METADATA_REFRESH_INTERVAL_MS_DEFAULT = 30000L;
    public static final String MIRROR_METADATA_REFRESH_INTERVAL_MS_DOC = "The interval in milliseconds at which the coordinator refreshes metadata from source clusters. " +
            "This controls how frequently the coordinator polls source clusters to detect new topics and metadata changes.";

    public static final String MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG = "mirror.failed.retry.initial.backoff.ms";
    public static final long MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DEFAULT = 1000L;
    public static final String MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DOC = "The initial backoff time in milliseconds before retrying a mirror partition " +
            "in FAILED state. The actual delay uses full jitter: a uniform random value in [0, backoff].";

    public static final String MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG = "mirror.failed.retry.max.backoff.ms";
    public static final long MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DEFAULT = 300000L;
    public static final String MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DOC = "The maximum backoff time in milliseconds for retrying a mirror partition in FAILED state.";

    public static final String MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG = "mirror.failed.retry.max.attempts";
    public static final int MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DEFAULT = 10;
    public static final String MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DOC = "The maximum number of automatic retry attempts for a mirror partition in FAILED state. " +
            "After this limit is reached, manual intervention is required via the start-mirror-topics command. " +
            "Set to 0 to disable automatic retries.";

    // ---------------------------------------------------------------
    // Mirror level configs: Set when creating/altering a specific mirror. Stored in cluster metadata.
    // ---------------------------------------------------------------

    // Connection configuration
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    public static final String BOOTSTRAP_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the source cluster. " +
            "This is a required configuration when creating a cluster mirror.";

    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final int REQUEST_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    public static final String CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    public static final long CONNECTION_SETUP_TIMEOUT_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS;
    public static final String CONNECTION_SETUP_TIMEOUT_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC;

    public static final String CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;
    public static final long CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;
    public static final String CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC;

    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;
    public static final long CONNECTIONS_MAX_IDLE_MS_DEFAULT = SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_DEFAULT;
    public static final String CONNECTIONS_MAX_IDLE_MS_DOC = SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_DOC;

    // Fetch and network configuration.
    // The replica.* equivalents use a mirror.* prefix to avoid ambiguity with broker-level configs.
    // The others reuse the existing key names since they have no replica.* prefix.
    public static final String MIRROR_FETCH_WAIT_MAX_MS_CONFIG = "mirror.fetch.wait.max.ms";
    public static final int MIRROR_FETCH_WAIT_MAX_MS_DEFAULT = 1000;
    public static final String MIRROR_FETCH_WAIT_MAX_MS_DOC = "The maximum wait time for each fetcher request issued by mirror replicas.";

    public static final String MIRROR_FETCH_BACKOFF_MS_CONFIG = "mirror.fetch.backoff.ms";
    public static final long MIRROR_FETCH_BACKOFF_MS_DEFAULT = 1000;
    public static final String MIRROR_FETCH_BACKOFF_MS_DOC = "The amount of time to wait before retrying mirror fetch requests after a failure. " +
            "This controls the backoff for mirror fetcher threads on connection errors or other exceptions from the source cluster.";

    public static final String MIRROR_FETCH_MIN_BYTES_CONFIG = "mirror.fetch.min.bytes";
    public static final int MIRROR_FETCH_MIN_BYTES_DEFAULT = 1;
    public static final String MIRROR_FETCH_MIN_BYTES_DOC = "Minimum bytes expected for each fetch response.";

    public static final String MIRROR_FETCH_RESPONSE_MAX_BYTES_CONFIG = "mirror.fetch.response.max.bytes";
    public static final int MIRROR_FETCH_RESPONSE_MAX_BYTES_DEFAULT = 10 * 1024 * 1024;
    public static final String MIRROR_FETCH_RESPONSE_MAX_BYTES_DOC = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
            "and if the first record batch in the first non-empty partition is larger than this value, the record batch will still be returned.";

    public static final String MIRROR_FETCH_MAX_BYTES_CONFIG = "mirror.fetch.max.bytes";
    public static final int MIRROR_FETCH_MAX_BYTES_DEFAULT = 1024 * 1024;
    public static final String MIRROR_FETCH_MAX_BYTES_DOC = "The number of bytes of messages to attempt to fetch for each partition.";

    public static final String MIRROR_SOCKET_TIMEOUT_MS_CONFIG = "mirror.socket.timeout.ms";
    public static final int MIRROR_SOCKET_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final String MIRROR_SOCKET_TIMEOUT_MS_DOC = "The socket timeout for network requests.";

    public static final String MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG = "mirror.socket.receive.buffer.bytes";
    public static final int MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT = 64 * 1024;
    public static final String MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The socket receive buffer for network requests to the leader for replicating data.";

    // Filter configuration
    public static final String MIRROR_TOPIC_PROPERTIES_EXCLUDE_CONFIG = "mirror.topic.properties.exclude";
    public static final String MIRROR_TOPIC_PROPERTIES_EXCLUDE_DEFAULT =
            "follower.replication.throttled.replicas,"
                    + "leader.replication.throttled.replicas,"
                    + "message.timestamp.difference.max.ms,"
                    + "log.message.timestamp.before.max.ms,"
                    + "log.message.timestamp.after.max.ms,"
                    + "message.timestamp.type,"
                    + "unclean.leader.election.enable,"
                    + "min.insync.replicas,"
                    + "mirror.name";
    public static final String MIRROR_TOPIC_PROPERTIES_EXCLUDE_DOC = "A comma-separated list of topic config property names to exclude from synchronization. "
            + "Properties in this list will not be replicated from the source cluster. "
            + "The mirror.name property is always excluded regardless of this setting.";

    public static final String MIRROR_TOPICS_INCLUDE_CONFIG = "mirror.topics.include";
    public static final String MIRROR_TOPICS_INCLUDE_DEFAULT = "";
    public static final String MIRROR_TOPICS_INCLUDE_DOC = "A comma-separated list of regex patterns for topic names to include in mirroring. "
            + "Topics on the source cluster whose names match at least one of the patterns will be automatically discovered and mirrored. "
            + "When empty (default), only explicitly added topics are mirrored.";

    public static final String MIRROR_TOPICS_EXCLUDE_CONFIG = "mirror.topics.exclude";
    public static final String MIRROR_TOPICS_EXCLUDE_DEFAULT = "__.*";
    public static final String MIRROR_TOPICS_EXCLUDE_DOC = "A comma-separated list of regex patterns for topic names to exclude from mirroring. "
            + "Topics matching the exclude pattern are not mirrored even if they match mirror.topics.include. "
            + "By default, internal topics (starting with '__') are excluded.";

    public static final String MIRROR_GROUPS_INCLUDE_CONFIG = "mirror.groups.include";
    public static final String MIRROR_GROUPS_INCLUDE_DEFAULT = ".*";
    public static final String MIRROR_GROUPS_INCLUDE_DOC = "A comma-separated list of regex patterns for consumer group IDs to include in offset synchronization. "
            + "Only consumer groups whose IDs match at least one of the patterns will have their offsets replicated from the source cluster.";

    public static final String MIRROR_GROUPS_EXCLUDE_CONFIG = "mirror.groups.exclude";
    public static final String MIRROR_GROUPS_EXCLUDE_DEFAULT = "";
    public static final String MIRROR_GROUPS_EXCLUDE_DOC = "A comma-separated list of regex patterns for consumer group IDs to exclude from offset synchronization. "
            + "Groups matching the exclude pattern are not replicated even if they match mirror.groups.include.";

    public static final String MIRROR_ACL_INCLUDE_CONFIG = "mirror.acl.include";
    public static final String MIRROR_ACL_INCLUDE_DEFAULT = "*";
    public static final String MIRROR_ACL_INCLUDE_DOC = "A comma-separated list of ACL include rules. Each rule uses semicolon-separated fields: "
            + "resourceType;resourceName;operation;permissionType;principal. Use '*' as wildcard for any field. The resourceName field supports "
            + "regex patterns. Trailing wildcard fields can be omitted. See AclRule javadoc for examples.";

    // Security configuration
    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    public static final String SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.name;
    public static final String SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;

    public static final String SASL_MECHANISM_CONFIG = SaslConfigs.SASL_MECHANISM;
    public static final String SASL_MECHANISM_DEFAULT = "PLAIN";
    public static final String SASL_MECHANISM_DOC = SaslConfigs.SASL_MECHANISM_DOC;

    public static final String SASL_JAAS_CONFIG = SaslConfigs.SASL_JAAS_CONFIG;
    public static final String SASL_JAAS_CONFIG_DOC = SaslConfigs.SASL_JAAS_CONFIG_DOC;

    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC;

    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC;

    public static final String SASL_LOGIN_CLASS = SaslConfigs.SASL_LOGIN_CLASS;
    public static final String SASL_LOGIN_CLASS_DOC = SaslConfigs.SASL_LOGIN_CLASS_DOC;

    public static final String SASL_KERBEROS_SERVICE_NAME = SaslConfigs.SASL_KERBEROS_SERVICE_NAME;
    public static final String SASL_KERBEROS_SERVICE_NAME_DOC = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC;

    public static final String SSL_PROTOCOL_CONFIG = SslConfigs.SSL_PROTOCOL_CONFIG;
    public static final String SSL_PROTOCOL_DEFAULT = SslConfigs.DEFAULT_SSL_PROTOCOL;
    public static final String SSL_PROTOCOL_DOC = SslConfigs.SSL_PROTOCOL_DOC;

    public static final String SSL_PROVIDER_CONFIG = SslConfigs.SSL_PROVIDER_CONFIG;
    public static final String SSL_PROVIDER_DOC = SslConfigs.SSL_PROVIDER_DOC;

    public static final String SSL_CIPHER_SUITES_CONFIG = SslConfigs.SSL_CIPHER_SUITES_CONFIG;
    public static final String SSL_CIPHER_SUITES_DOC = SslConfigs.SSL_CIPHER_SUITES_DOC;

    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
    public static final String SSL_ENABLED_PROTOCOLS_DOC = SslConfigs.SSL_ENABLED_PROTOCOLS_DOC;

    public static final String SSL_KEYSTORE_TYPE_CONFIG = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
    public static final String SSL_KEYSTORE_TYPE_DEFAULT = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE;
    public static final String SSL_KEYSTORE_TYPE_DOC = SslConfigs.SSL_KEYSTORE_TYPE_DOC;

    public static final String SSL_KEYSTORE_LOCATION_CONFIG = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
    public static final String SSL_KEYSTORE_LOCATION_DOC = SslConfigs.SSL_KEYSTORE_LOCATION_DOC;

    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
    public static final String SSL_KEYSTORE_PASSWORD_DOC = SslConfigs.SSL_KEYSTORE_PASSWORD_DOC;

    public static final String SSL_KEY_PASSWORD_CONFIG = SslConfigs.SSL_KEY_PASSWORD_CONFIG;
    public static final String SSL_KEY_PASSWORD_DOC = SslConfigs.SSL_KEY_PASSWORD_DOC;

    public static final String SSL_KEYSTORE_KEY_CONFIG = SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
    public static final String SSL_KEYSTORE_KEY_DOC = SslConfigs.SSL_KEYSTORE_KEY_DOC;

    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC;

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
    public static final String SSL_TRUSTSTORE_TYPE_DEFAULT = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE;
    public static final String SSL_TRUSTSTORE_TYPE_DOC = SslConfigs.SSL_TRUSTSTORE_TYPE_DOC;

    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
    public static final String SSL_TRUSTSTORE_LOCATION_DOC = SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC;

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC = SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC;

    public static final String SSL_TRUSTSTORE_CERTIFICATES_CONFIG = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
    public static final String SSL_TRUSTSTORE_CERTIFICATES_DOC = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC;

    public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
    public static final String SSL_KEYMANAGER_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM;
    public static final String SSL_KEYMANAGER_ALGORITHM_DOC = SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC;

    public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM;
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DOC = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC;

    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM;
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC;

    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_DOC = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC;

    public static final String SSL_ENGINE_FACTORY_CLASS_CONFIG = SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;
    public static final String SSL_ENGINE_FACTORY_CLASS_DOC = SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC;

    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    public static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /**
     * Configuration definition for Cluster Mirroring feature.
     * This includes all supported configurations with proper validation, defaults, and documentation.
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Connection
            .define(BOOTSTRAP_SERVERS_CONFIG, LIST, null, HIGH, BOOTSTRAP_SERVERS_DOC)
            .define(REQUEST_TIMEOUT_MS_CONFIG, INT, REQUEST_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, REQUEST_TIMEOUT_MS_DOC)
            .define(CONNECTION_SETUP_TIMEOUT_MS_CONFIG, LONG, CONNECTION_SETUP_TIMEOUT_MS_DEFAULT, atLeast(0L), MEDIUM, CONNECTION_SETUP_TIMEOUT_MS_DOC)
            .define(CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, LONG, CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT, atLeast(0L), MEDIUM, CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
            .define(CONNECTIONS_MAX_IDLE_MS_CONFIG, LONG, CONNECTIONS_MAX_IDLE_MS_DEFAULT, MEDIUM, CONNECTIONS_MAX_IDLE_MS_DOC)
            // Fetch and network
            .define(MIRROR_FETCH_WAIT_MAX_MS_CONFIG, INT, MIRROR_FETCH_WAIT_MAX_MS_DEFAULT, atLeast(0), MEDIUM, MIRROR_FETCH_WAIT_MAX_MS_DOC)
            .define(MIRROR_FETCH_BACKOFF_MS_CONFIG, LONG, MIRROR_FETCH_BACKOFF_MS_DEFAULT, atLeast(0L), MEDIUM, MIRROR_FETCH_BACKOFF_MS_DOC)
            .define(MIRROR_FETCH_MIN_BYTES_CONFIG, INT, MIRROR_FETCH_MIN_BYTES_DEFAULT, atLeast(0), MEDIUM, MIRROR_FETCH_MIN_BYTES_DOC)
            .define(MIRROR_FETCH_RESPONSE_MAX_BYTES_CONFIG, INT, MIRROR_FETCH_RESPONSE_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, MIRROR_FETCH_RESPONSE_MAX_BYTES_DOC)
            .define(MIRROR_FETCH_MAX_BYTES_CONFIG, INT, MIRROR_FETCH_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, MIRROR_FETCH_MAX_BYTES_DOC)
            .define(MIRROR_SOCKET_TIMEOUT_MS_CONFIG, INT, MIRROR_SOCKET_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, MIRROR_SOCKET_TIMEOUT_MS_DOC)
            .define(MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, INT, MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT, atLeast(0), MEDIUM, MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_DOC)
            // Filter
            .define(MIRROR_TOPIC_PROPERTIES_EXCLUDE_CONFIG, LIST, MIRROR_TOPIC_PROPERTIES_EXCLUDE_DEFAULT, LOW, MIRROR_TOPIC_PROPERTIES_EXCLUDE_DOC)
            .define(MIRROR_TOPICS_INCLUDE_CONFIG, LIST, MIRROR_TOPICS_INCLUDE_DEFAULT, LOW, MIRROR_TOPICS_INCLUDE_DOC)
            .define(MIRROR_TOPICS_EXCLUDE_CONFIG, LIST, MIRROR_TOPICS_EXCLUDE_DEFAULT, LOW, MIRROR_TOPICS_EXCLUDE_DOC)
            .define(MIRROR_GROUPS_INCLUDE_CONFIG, LIST, MIRROR_GROUPS_INCLUDE_DEFAULT, LOW, MIRROR_GROUPS_INCLUDE_DOC)
            .define(MIRROR_GROUPS_EXCLUDE_CONFIG, LIST, MIRROR_GROUPS_EXCLUDE_DEFAULT, LOW, MIRROR_GROUPS_EXCLUDE_DOC)
            .define(MIRROR_ACL_INCLUDE_CONFIG, LIST, MIRROR_ACL_INCLUDE_DEFAULT, LOW, MIRROR_ACL_INCLUDE_DOC)
            // Security
            .define(SECURITY_PROTOCOL_CONFIG, STRING, SECURITY_PROTOCOL_DEFAULT, ConfigDef.ValidString.in(SecurityProtocol.PLAINTEXT.name, SecurityProtocol.SSL.name,
                    SecurityProtocol.SASL_PLAINTEXT.name, SecurityProtocol.SASL_SSL.name), MEDIUM, SECURITY_PROTOCOL_DOC)
            .define(SASL_MECHANISM_CONFIG, STRING, SASL_MECHANISM_DEFAULT, MEDIUM, SASL_MECHANISM_DOC)
            .define(SASL_JAAS_CONFIG, PASSWORD, null, MEDIUM, SASL_JAAS_CONFIG_DOC)
            .define(SASL_CLIENT_CALLBACK_HANDLER_CLASS, STRING, null, LOW, SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
            .define(SASL_LOGIN_CALLBACK_HANDLER_CLASS, STRING, null, LOW, SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
            .define(SASL_LOGIN_CLASS, STRING, null, LOW, SASL_LOGIN_CLASS_DOC)
            .define(SASL_KERBEROS_SERVICE_NAME, STRING, null, MEDIUM, SASL_KERBEROS_SERVICE_NAME_DOC)
            .define(SSL_PROTOCOL_CONFIG, STRING, SSL_PROTOCOL_DEFAULT, MEDIUM, SSL_PROTOCOL_DOC)
            .define(SSL_PROVIDER_CONFIG, STRING, null, LOW, SSL_PROVIDER_DOC)
            .define(SSL_CIPHER_SUITES_CONFIG, LIST, null, LOW, SSL_CIPHER_SUITES_DOC)
            .define(SSL_ENABLED_PROTOCOLS_CONFIG, LIST, null, MEDIUM, SSL_ENABLED_PROTOCOLS_DOC)
            .define(SSL_KEYSTORE_TYPE_CONFIG, STRING, SSL_KEYSTORE_TYPE_DEFAULT, MEDIUM, SSL_KEYSTORE_TYPE_DOC)
            .define(SSL_KEYSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, SSL_KEYSTORE_LOCATION_DOC)
            .define(SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SSL_KEYSTORE_PASSWORD_DOC)
            .define(SSL_KEY_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SSL_KEY_PASSWORD_DOC)
            .define(SSL_KEYSTORE_KEY_CONFIG, PASSWORD, null, MEDIUM, SSL_KEYSTORE_KEY_DOC)
            .define(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, PASSWORD, null, MEDIUM, SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC)
            .define(SSL_TRUSTSTORE_TYPE_CONFIG, STRING, SSL_TRUSTSTORE_TYPE_DEFAULT, MEDIUM, SSL_TRUSTSTORE_TYPE_DOC)
            .define(SSL_TRUSTSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, SSL_TRUSTSTORE_LOCATION_DOC)
            .define(SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SSL_TRUSTSTORE_PASSWORD_DOC)
            .define(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, PASSWORD, null, MEDIUM, SSL_TRUSTSTORE_CERTIFICATES_DOC)
            .define(SSL_KEYMANAGER_ALGORITHM_CONFIG, STRING, SSL_KEYMANAGER_ALGORITHM_DEFAULT, LOW, SSL_KEYMANAGER_ALGORITHM_DOC)
            .define(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, STRING, SSL_TRUSTMANAGER_ALGORITHM_DEFAULT, LOW, SSL_TRUSTMANAGER_ALGORITHM_DOC)
            .define(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, STRING, SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT, LOW, SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
            .define(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, STRING, null, LOW, SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
            .define(SSL_ENGINE_FACTORY_CLASS_CONFIG, STRING, null, LOW, SSL_ENGINE_FACTORY_CLASS_DOC)
            .define(SECURITY_PROVIDERS_CONFIG, STRING, null, LOW, SECURITY_PROVIDERS_DOC);

    private final AbstractConfig config;
    private final Pattern topicPropertiesExcludePattern;
    private final Pattern topicsIncludePattern;
    private final Pattern topicsExcludePattern;
    private final Pattern groupsIncludePattern;
    private final Pattern groupsExcludePattern;
    private final List<MirrorFilterUtils.AclRule> aclIncludeRules;
    private final String securityProtocol;
    private final String saslMechanism;

    /**
     * Creates a ClusterMirrorConfig from mirror metadata properties stored in the cluster metadata topic.
     * Filter configs are loaded from the metadata, with defaults applied for any missing values.
     *
     * @param properties the mirror metadata properties
     * @return a new ClusterMirrorConfig instance
     */
    public static ClusterMirrorConfig fromProperties(Properties properties) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, properties, false) { };
        return new ClusterMirrorConfig(config,
                config.getList(MIRROR_TOPIC_PROPERTIES_EXCLUDE_CONFIG),
                config.getList(MIRROR_TOPICS_INCLUDE_CONFIG),
                config.getList(MIRROR_TOPICS_EXCLUDE_CONFIG),
                config.getList(MIRROR_GROUPS_INCLUDE_CONFIG),
                config.getList(MIRROR_GROUPS_EXCLUDE_CONFIG),
                config.getList(MIRROR_ACL_INCLUDE_CONFIG));
    }

    private ClusterMirrorConfig(AbstractConfig config,
                         List<String> topicPropertiesExclude,
                         List<String> topicsInclude,
                         List<String> topicsExclude,
                         List<String> groupsInclude,
                         List<String> groupsExclude,
                         List<String> aclInclude) {
        this.config = config;
        this.securityProtocol = config.getString(SECURITY_PROTOCOL_CONFIG);
        this.saslMechanism = config.getString(SASL_MECHANISM_CONFIG);
        this.topicPropertiesExcludePattern = MirrorFilterUtils.compilePatternList(topicPropertiesExclude);
        this.topicsIncludePattern = MirrorFilterUtils.compilePatternList(topicsInclude);
        this.topicsExcludePattern = MirrorFilterUtils.compilePatternList(topicsExclude);
        this.groupsIncludePattern = MirrorFilterUtils.compilePatternList(groupsInclude);
        this.groupsExcludePattern = MirrorFilterUtils.compilePatternList(groupsExclude);
        this.aclIncludeRules = MirrorFilterUtils.parseAclRules(aclInclude);
    }

    /**
     * Creates a ClusterMirrorConfig from broker-level config.
     * Filter configs are not available at broker level, so defaults are used.
     */
    public ClusterMirrorConfig(AbstractConfig config) {
        this.config = config;
        this.securityProtocol = null;
        this.saslMechanism = null;
        this.topicPropertiesExcludePattern = MirrorFilterUtils.compilePatternList(Arrays.asList(MIRROR_TOPIC_PROPERTIES_EXCLUDE_DEFAULT.split(",")));
        this.topicsIncludePattern = null;
        this.topicsExcludePattern = MirrorFilterUtils.compilePatternList(List.of(MIRROR_TOPICS_EXCLUDE_DEFAULT));
        this.groupsIncludePattern = MirrorFilterUtils.compilePatternList(List.of(MIRROR_GROUPS_INCLUDE_DEFAULT));
        this.groupsExcludePattern = null;
        this.aclIncludeRules = MirrorFilterUtils.parseAclRules(List.of(MIRROR_ACL_INCLUDE_DEFAULT));
    }

    public Pattern topicPropertiesExcludePattern() {
        return topicPropertiesExcludePattern;
    }

    public Pattern topicsIncludePattern() {
        return topicsIncludePattern;
    }

    public Pattern topicsExcludePattern() {
        return topicsExcludePattern;
    }

    public Pattern groupsIncludePattern() {
        return groupsIncludePattern;
    }

    public Pattern groupsExcludePattern() {
        return groupsExcludePattern;
    }

    public List<MirrorFilterUtils.AclRule> aclIncludeRules() {
        return aclIncludeRules;
    }

    public int stateTopicNumPartitions() {
        return config.getInt(MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG);
    }

    public short stateTopicReplicationFactor() {
        return config.getShort(MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG);
    }

    public String securityProtocol() {
        return securityProtocol;
    }

    public String saslMechanism() {
        return saslMechanism;
    }

    public int numReplicaFetchers() {
        return config.getInt(MIRROR_NUM_REPLICA_FETCHERS_CONFIG);
    }

    public long metadataRefreshIntervalMs() {
        return config.getLong(MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG);
    }

    public long fetchBackoffMs() {
        return config.getLong(MIRROR_FETCH_BACKOFF_MS_CONFIG);
    }

    public long failedRetryInitialBackoffMs() {
        return config.getLong(MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG);
    }

    public long failedRetryMaxBackoffMs() {
        return config.getLong(MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG);
    }

    public int failedRetryMaxAttempts() {
        return config.getInt(MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG);
    }

    public int fetchWaitMaxMs() {
        return config.getInt(MIRROR_FETCH_WAIT_MAX_MS_CONFIG);
    }

    public int fetchMinBytes() {
        return config.getInt(MIRROR_FETCH_MIN_BYTES_CONFIG);
    }

    public int fetchResponseMaxBytes() {
        return config.getInt(MIRROR_FETCH_RESPONSE_MAX_BYTES_CONFIG);
    }

    public int fetchMaxBytes() {
        return config.getInt(MIRROR_FETCH_MAX_BYTES_CONFIG);
    }

    public int socketTimeoutMs() {
        return config.getInt(MIRROR_SOCKET_TIMEOUT_MS_CONFIG);
    }

    public int socketReceiveBufferBytes() {
        return config.getInt(MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG);
    }

    public int requestTimeoutMs() {
        return config.getInt(REQUEST_TIMEOUT_MS_CONFIG);
    }

    public long connectionSetupTimeoutMs() {
        return config.getLong(CONNECTION_SETUP_TIMEOUT_MS_CONFIG);
    }

    public long connectionSetupTimeoutMaxMs() {
        return config.getLong(CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG);
    }

    public long connectionsMaxIdleMs() {
        return config.getLong(CONNECTIONS_MAX_IDLE_MS_CONFIG);
    }

    /**
     * Returns the underlying AbstractConfig instance.
     * This provides access to all configuration values including those processed by config providers.
     *
     * @return the underlying AbstractConfig
     */
    public AbstractConfig getConfig() {
        return config;
    }

    /**
     * Creates a ConfigDef for broker-level mirror configurations.
     * This is merged into AbstractKafkaConfig for broker config validation.
     */
    public static ConfigDef brokerConfigDef() {
        return new ConfigDef()
                .define(MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG, INT, MIRROR_STATE_TOPIC_NUM_PARTITIONS_DEFAULT, atLeast(1), HIGH, MIRROR_STATE_TOPIC_NUM_PARTITIONS_DOC)
                .define(MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DOC)
                .define(MIRROR_NUM_REPLICA_FETCHERS_CONFIG, INT, MIRROR_NUM_REPLICA_FETCHERS_DEFAULT, atLeast(1), HIGH, MIRROR_NUM_REPLICA_FETCHERS_DOC)
                .define(MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG, LONG, MIRROR_METADATA_REFRESH_INTERVAL_MS_DEFAULT, atLeast(0L), MEDIUM, MIRROR_METADATA_REFRESH_INTERVAL_MS_DOC)
                .define(MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG, LONG, MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DEFAULT, atLeast(1L), MEDIUM, MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DOC)
                .define(MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG, LONG, MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DEFAULT, atLeast(1L), MEDIUM, MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DOC)
                .define(MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG, INT, MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DEFAULT, atLeast(0), MEDIUM, MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DOC)
                .define(MIRROR_FETCH_BACKOFF_MS_CONFIG, LONG, MIRROR_FETCH_BACKOFF_MS_DEFAULT, atLeast(0L), MEDIUM, MIRROR_FETCH_BACKOFF_MS_DOC)
                .define(MIRROR_FETCH_WAIT_MAX_MS_CONFIG, INT, MIRROR_FETCH_WAIT_MAX_MS_DEFAULT, atLeast(0), MEDIUM, MIRROR_FETCH_WAIT_MAX_MS_DOC)
                .define(MIRROR_FETCH_MIN_BYTES_CONFIG, INT, MIRROR_FETCH_MIN_BYTES_DEFAULT, atLeast(0), MEDIUM, MIRROR_FETCH_MIN_BYTES_DOC)
                .define(MIRROR_FETCH_RESPONSE_MAX_BYTES_CONFIG, INT, MIRROR_FETCH_RESPONSE_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, MIRROR_FETCH_RESPONSE_MAX_BYTES_DOC)
                .define(MIRROR_FETCH_MAX_BYTES_CONFIG, INT, MIRROR_FETCH_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, MIRROR_FETCH_MAX_BYTES_DOC)
                .define(MIRROR_SOCKET_TIMEOUT_MS_CONFIG, INT, MIRROR_SOCKET_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, MIRROR_SOCKET_TIMEOUT_MS_DOC)
                .define(MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, INT, MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT, atLeast(0), MEDIUM, MIRROR_SOCKET_RECEIVE_BUFFER_BYTES_DOC);
    }
}
