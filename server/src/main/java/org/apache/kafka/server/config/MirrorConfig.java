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
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
public class MirrorConfig {
    // Internal topic configuration
    public static final String MIRROR_TOPIC_NUM_PARTITIONS_CONFIG = "mirror.topic.num.partitions";
    public static final int MIRROR_TOPIC_NUM_PARTITIONS_DEFAULT = 3; // TODO restore 50 after POC testing
    public static final String MIRROR_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions for the Cluster Mirror internal topic (should not change after deployment).";

    public static final String MIRROR_TOPIC_REPLICATION_FACTOR_CONFIG = "mirror.topic.replication.factor";
    public static final short MIRROR_TOPIC_REPLICATION_FACTOR_DEFAULT = 1; // TODO restore 3 after POC testing
    public static final String MIRROR_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the Cluster Mirror internal topic. " +
            "Topic creation will fail until the cluster size meets this replication factor requirement.";

    // Topic properties include filter (regex patterns)
    public static final String PROPERTIES_INCLUDE_CONFIG = "mirror.properties.include";
    public static final String PROPERTIES_INCLUDE_DEFAULT = ".*";
    public static final String PROPERTIES_INCLUDE_DOC = "A comma-separated list of regex patterns for topic config properties to include in synchronization. "
            + "Only properties whose names match at least one of the patterns will be replicated from the source cluster. "
            + "Properties in the internal exclude list are always excluded regardless of this setting.";

    // Properties that should never be synced regardless of the include pattern
    private static final Set<String> PROPERTIES_EXCLUDE = Set.of(
            "follower.replication.throttled.replicas",
            "leader.replication.throttled.replicas",
            "message.timestamp.difference.max.ms",
            "message.timestamp.type",
            "unclean.leader.election.enable",
            "min.insync.replicas"
    );

    // Consumer group include filter (regex patterns)
    public static final String GROUPS_INCLUDE_CONFIG = "mirror.groups.include";
    public static final String GROUPS_INCLUDE_DEFAULT = ".*";
    public static final String GROUPS_INCLUDE_DOC = "A comma-separated list of regex patterns for consumer group IDs to include in offset synchronization. "
            + "Only consumer groups whose IDs match at least one of the patterns will have their offsets replicated from the source cluster.";

    // ACL include filter (semicolon-separated rules)
    public static final String ACL_INCLUDE_CONFIG = "mirror.acl.include";
    public static final String ACL_INCLUDE_DEFAULT = "*";
    public static final String ACL_INCLUDE_DOC = "A comma-separated list of ACL include rules. Each rule uses semicolon-separated fields: "
            + "resourceType;resourceName;operation;permissionType;principal. Use '*' as wildcard for any field. The resourceName field supports "
            + "regex patterns. Trailing wildcard fields can be omitted. See AclRule javadoc for examples.";

    // Fetcher configuration
    public static final String NUM_REPLICA_FETCHERS_CONFIG = "mirror.num.replica.fetchers";
    public static final int NUM_REPLICA_FETCHERS_DEFAULT = 1;
    public static final String NUM_REPLICA_FETCHERS_DOC = "Number of fetcher threads used to replicate records from remote clusters via cluster mirror. " +
            "The total number of remote fetchers on each broker is bound by <code>mirror.num.replica.fetchers</code> multiplied by the number of cluster mirrors. " +
            "Increasing this value can increase the degree of I/O parallelism for cross-cluster replication at the cost of higher CPU and memory utilization.";

    // Metadata refresh interval
    public static final String METADATA_REFRESH_INTERVAL_MS_CONFIG = "mirror.metadata.refresh.interval.ms";
    public static final long METADATA_REFRESH_INTERVAL_MS_DEFAULT = 30000L; // 30 seconds
    public static final String METADATA_REFRESH_INTERVAL_MS_DOC = "The interval in milliseconds at which the coordinator refreshes metadata from source clusters. " +
            "This controls how frequently the coordinator polls source clusters to detect new topics and metadata changes.";

    // Connection configuration
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    public static final String BOOTSTRAP_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster for Cluster Mirror.";

    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    public static final long METADATA_MAX_AGE_DEFAULT = 300000L; // 5 minutes
    public static final String METADATA_MAX_AGE_DOC = "The period of time in milliseconds after which we force a refresh of metadata for Cluster Mirror connections.";

    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;
    public static final int SEND_BUFFER_DEFAULT = 131072; // 128KB
    public static final String SEND_BUFFER_DOC = "The size of the TCP send buffer (SO_SNDBUF) to use when sending data for Cluster Mirror connections.";

    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;
    public static final int RECEIVE_BUFFER_DEFAULT = 65536; // 64KB
    public static final String RECEIVE_BUFFER_DOC = "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data for Cluster Mirror connections.";

    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final int REQUEST_TIMEOUT_MS_DEFAULT = 30000; // 30 seconds
    public static final String REQUEST_TIMEOUT_MS_DOC = "The configuration controls the maximum amount of time the client will wait for the response of a request for Cluster Mirror connections.";

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    public static final long SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS; // 10 seconds
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC = "The amount of time the client will wait for the socket connection to be established for Cluster Mirror connections.";

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;
    public static final long SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS; // 30 seconds
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = "The maximum amount of time the client will wait for the socket connection to be established for Cluster Mirror connections.";

    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;
    public static final long RECONNECT_BACKOFF_MS_DEFAULT = 50L;
    public static final String RECONNECT_BACKOFF_MS_DOC = "The base amount of time to wait before attempting to reconnect to a given host for Cluster Mirror connections.";

    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;
    public static final long RECONNECT_BACKOFF_MAX_MS_DEFAULT = 1000L; // 1 second
    public static final String RECONNECT_BACKOFF_MAX_MS_DOC = "The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly failed to connect for Cluster Mirror connections.";

    public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;
    public static final int RETRIES_DEFAULT = Integer.MAX_VALUE;
    public static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to resend any request that fails with a potentially transient error for Cluster Mirror connections.";

    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
    public static final long RETRY_BACKOFF_MS_DEFAULT = 100L;
    public static final String RETRY_BACKOFF_MS_DOC = "The amount of time to wait before attempting to retry a failed request to a given topic partition for Cluster Mirror connections.";

    // Security configuration
    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    public static final String SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.name;
    public static final String SECURITY_PROTOCOL_DOC = "Protocol used to communicate with remote brokers for Cluster Mirror. " +
            "Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.";

    // SSL configuration
    public static final String SSL_PROTOCOL_CONFIG = SslConfigs.SSL_PROTOCOL_CONFIG;
    public static final String SSL_PROTOCOL_DEFAULT = SslConfigs.DEFAULT_SSL_PROTOCOL;
    public static final String SSL_PROTOCOL_DOC = "The SSL protocol used for Cluster Mirror connections.";

    public static final String SSL_PROVIDER_CONFIG = SslConfigs.SSL_PROVIDER_CONFIG;
    public static final String SSL_PROVIDER_DOC = "The name of the security provider used for SSL connections for Cluster Mirror.";

    public static final String SSL_CIPHER_SUITES_CONFIG = SslConfigs.SSL_CIPHER_SUITES_CONFIG;
    public static final String SSL_CIPHER_SUITES_DOC = "A list of cipher suites for Cluster Mirror SSL connections.";

    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
    public static final String SSL_ENABLED_PROTOCOLS_DOC = "The list of protocols enabled for SSL connections for Cluster Mirror.";

    public static final String SSL_KEYSTORE_TYPE_CONFIG = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
    public static final String SSL_KEYSTORE_TYPE_DEFAULT = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE;
    public static final String SSL_KEYSTORE_TYPE_DOC = "The file format of the key store file for Cluster Mirror SSL connections.";

    public static final String SSL_KEYSTORE_LOCATION_CONFIG = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
    public static final String SSL_KEYSTORE_LOCATION_DOC = "The location of the key store file for Cluster Mirror SSL connections.";

    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
    public static final String SSL_KEYSTORE_PASSWORD_DOC = "The store password for the key store file for Cluster Mirror SSL connections.";

    public static final String SSL_KEY_PASSWORD_CONFIG = SslConfigs.SSL_KEY_PASSWORD_CONFIG;
    public static final String SSL_KEY_PASSWORD_DOC = "The password of the private key in the key store file for Cluster Mirror SSL connections.";

    public static final String SSL_KEYSTORE_KEY_CONFIG = SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
    public static final String SSL_KEYSTORE_KEY_DOC = "Private key in the format specified by 'ssl.keystore.type' for Cluster Mirror SSL connections.";

    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC = "Certificate chain in the format specified by 'ssl.keystore.type' for Cluster Mirror SSL connections.";

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
    public static final String SSL_TRUSTSTORE_TYPE_DEFAULT = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE;
    public static final String SSL_TRUSTSTORE_TYPE_DOC = "The file format of the trust store file for Cluster Mirror SSL connections.";

    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
    public static final String SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file for Cluster Mirror SSL connections.";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the trust store file for Cluster Mirror SSL connections.";

    public static final String SSL_TRUSTSTORE_CERTIFICATES_CONFIG = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
    public static final String SSL_TRUSTSTORE_CERTIFICATES_DOC = "Trusted certificates in the format specified by 'ssl.truststore.type' for Cluster Mirror SSL connections.";

    public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
    public static final String SSL_KEYMANAGER_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM;
    public static final String SSL_KEYMANAGER_ALGORITHM_DOC = "The algorithm used by key manager factory for SSL connections for Cluster Mirror.";

    public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM;
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DOC = "The algorithm used by trust manager factory for SSL connections for Cluster Mirrors";

    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM;
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = "The endpoint identification algorithm to validate server hostname using server certificate for Cluster Mirror SSL connections.";

    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_DOC = "The SecureRandom PRNG implementation to use for SSL cryptography operations for Cluster Mirror.";

    public static final String SSL_ENGINE_FACTORY_CLASS_CONFIG = SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;
    public static final String SSL_ENGINE_FACTORY_CLASS_DOC = "The class of type org.apache.kafka.common.security.auth.SslEngineFactory to provide SSLEngine objects for Cluster Mirror SSL connections.";

    // SASL configuration
    public static final String SASL_MECHANISM_CONFIG = SaslConfigs.SASL_MECHANISM;
    public static final String SASL_MECHANISM_DEFAULT = "PLAIN";
    public static final String SASL_MECHANISM_DOC = "SASL mechanism used for Cluster Mirror authentication.";

    public static final String SASL_JAAS_CONFIG = SaslConfigs.SASL_JAAS_CONFIG;
    public static final String SASL_JAAS_CONFIG_DOC = "JAAS login context parameters for SASL connections in the format used by JAAS configuration files.";

    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC = "The fully qualified name of a SASL client callback handler class that implements the AuthenticateCallbackHandler interface for Cluster Mirror connections.";

    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC = "The fully qualified name of a SASL login callback handler class that implements the AuthenticateCallbackHandler interface for Cluster Mirror connections.";

    public static final String SASL_LOGIN_CLASS = SaslConfigs.SASL_LOGIN_CLASS;
    public static final String SASL_LOGIN_CLASS_DOC = "The fully qualified name of a class that implements the Login interface for Cluster Mirror connections.";

    public static final String SASL_KERBEROS_SERVICE_NAME = SaslConfigs.SASL_KERBEROS_SERVICE_NAME;
    public static final String SASL_KERBEROS_SERVICE_NAME_DOC = "The Kerberos principal name that Kafka runs as for Cluster Mirror connections.";

    /**
     * Configuration definition for Cluster Mirror feature.
     * This includes all supported configurations with proper validation, defaults, and documentation.
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PROPERTIES_INCLUDE_CONFIG, LIST, PROPERTIES_INCLUDE_DEFAULT, LOW, PROPERTIES_INCLUDE_DOC)
            .define(GROUPS_INCLUDE_CONFIG, LIST, GROUPS_INCLUDE_DEFAULT, LOW, GROUPS_INCLUDE_DOC)
            .define(ACL_INCLUDE_CONFIG, LIST, ACL_INCLUDE_DEFAULT, LOW, ACL_INCLUDE_DOC)
            .define(MIRROR_TOPIC_NUM_PARTITIONS_CONFIG, INT, MIRROR_TOPIC_NUM_PARTITIONS_DEFAULT, atLeast(1), HIGH, MIRROR_TOPIC_NUM_PARTITIONS_DOC)
            .define(MIRROR_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, MIRROR_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, MIRROR_TOPIC_REPLICATION_FACTOR_DOC)
            .define(NUM_REPLICA_FETCHERS_CONFIG, INT, NUM_REPLICA_FETCHERS_DEFAULT, atLeast(1), HIGH, NUM_REPLICA_FETCHERS_DOC)
            .define(METADATA_REFRESH_INTERVAL_MS_CONFIG, LONG, METADATA_REFRESH_INTERVAL_MS_DEFAULT, atLeast(0L), MEDIUM, METADATA_REFRESH_INTERVAL_MS_DOC)
            .define(BOOTSTRAP_SERVERS_CONFIG, LIST, null, HIGH, BOOTSTRAP_SERVERS_DOC)
            .define(METADATA_MAX_AGE_CONFIG, LONG, METADATA_MAX_AGE_DEFAULT, atLeast(0), LOW, METADATA_MAX_AGE_DOC)
            .define(SEND_BUFFER_CONFIG, INT, SEND_BUFFER_DEFAULT, atLeast(-1), MEDIUM, SEND_BUFFER_DOC)
            .define(RECEIVE_BUFFER_CONFIG, INT, RECEIVE_BUFFER_DEFAULT, atLeast(-1), MEDIUM, RECEIVE_BUFFER_DOC)
            .define(REQUEST_TIMEOUT_MS_CONFIG, INT, REQUEST_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, REQUEST_TIMEOUT_MS_DOC)
            .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, LONG, SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DEFAULT, atLeast(0L), MEDIUM, SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
            .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, LONG, SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT, atLeast(0L), MEDIUM, SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
            .define(RECONNECT_BACKOFF_MS_CONFIG, LONG, RECONNECT_BACKOFF_MS_DEFAULT, atLeast(0L), LOW, RECONNECT_BACKOFF_MS_DOC)
            .define(RECONNECT_BACKOFF_MAX_MS_CONFIG, LONG, RECONNECT_BACKOFF_MAX_MS_DEFAULT, atLeast(0L), LOW, RECONNECT_BACKOFF_MAX_MS_DOC)
            .define(RETRIES_CONFIG, INT, RETRIES_DEFAULT, ConfigDef.Range.between(0, Integer.MAX_VALUE), LOW, RETRIES_DOC)
            .define(RETRY_BACKOFF_MS_CONFIG, LONG, RETRY_BACKOFF_MS_DEFAULT, atLeast(0L), LOW, RETRY_BACKOFF_MS_DOC)
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
            .define(SSL_ENGINE_FACTORY_CLASS_CONFIG, STRING, null, LOW, SSL_ENGINE_FACTORY_CLASS_DOC);

    private final AbstractConfig config;
    private final Pattern propertiesIncludePattern;
    private final Pattern groupsIncludePattern;
    private final List<AclRule> aclIncludeRules;
    private final int mirrorTopicNumPartitions;
    private final short mirrorTopicReplicationFactor;
    private final String securityProtocol;
    private final String saslMechanism;

    public MirrorConfig(AbstractConfig config) {
        this.config = config;
        propertiesIncludePattern = compilePatternList(config.getList(PROPERTIES_INCLUDE_CONFIG));
        groupsIncludePattern = compilePatternList(config.getList(GROUPS_INCLUDE_CONFIG));
        aclIncludeRules = parseAclRules(config.getList(ACL_INCLUDE_CONFIG));
        mirrorTopicNumPartitions = config.getInt(MIRROR_TOPIC_NUM_PARTITIONS_CONFIG);
        mirrorTopicReplicationFactor = config.getShort(MIRROR_TOPIC_REPLICATION_FACTOR_CONFIG);
        securityProtocol = config.getString(SECURITY_PROTOCOL_CONFIG);
        saslMechanism = config.getString(SASL_MECHANISM_CONFIG);
    }

    /**
     * Creates a MirrorConfig instance from Properties, using AbstractConfig's
     * built-in config provider resolution and password handling.
     *
     * @param properties the raw properties
     * @return a new MirrorConfig instance with processed properties
     */
    public static MirrorConfig fromProperties(Properties properties) {
        // AbstractConfig handles config provider resolution and password conversion
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, properties, false) { };
        return new MirrorConfig(config);
    }

    public Pattern propertiesIncludePattern() {
        return propertiesIncludePattern;
    }

    public static Set<String> propertiesExclude() {
        return PROPERTIES_EXCLUDE;
    }

    public Pattern groupsIncludePattern() {
        return groupsIncludePattern;
    }

    public List<AclRule> aclIncludeRules() {
        return aclIncludeRules;
    }

    public int mirrorTopicNumPartitions() {
        return mirrorTopicNumPartitions;
    }

    public short mirrorTopicReplicationFactor() {
        return mirrorTopicReplicationFactor;
    }

    public String securityProtocol() {
        return securityProtocol;
    }

    public String saslMechanism() {
        return saslMechanism;
    }

    public int numReplicaFetchers() {
        return config.getInt(NUM_REPLICA_FETCHERS_CONFIG);
    }

    public long metadataRefreshIntervalMs() {
        return config.getLong(METADATA_REFRESH_INTERVAL_MS_CONFIG);
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
     * Creates a ConfigDef that can be used to validate Cluster Mirror properties.
     * This is useful for components that need to validate Cluster Mirror configurations
     * without creating a full MirrorConfig instance.
     *
     * @return ConfigDef for Cluster Mirror configurations
     */
    public static ConfigDef configDef() {
        return CONFIG_DEF;
    }

    /**
     * Compiles a list of regex pattern strings into a single {@link Pattern} by joining them with {@code |}.
     *
     * @param patterns the list of regex pattern strings
     * @return a compiled Pattern that matches any of the given patterns
     */
    public static Pattern compilePatternList(List<String> patterns) {
        String combined = patterns.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("|"));
        return Pattern.compile(combined);
    }

    /**
     * Parses a list of ACL rule strings into a list of {@link AclRule} instances.
     *
     * @param rules the list of rule strings in semicolon-separated format
     * @return parsed list of AclRule instances
     */
    public static List<AclRule> parseAclRules(List<String> rules) {
        return rules.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(AclRule::parse)
                .toList();
    }

    /**
     * Creates a ConfigDef that includes broker-level mirror configurations.
     * This is used in AbstractKafkaConfig to merge mirror configurations into the broker config.
     *
     * @return ConfigDef containing broker-level mirror configurations
     */
    public static ConfigDef topicConfigDef() {
        return new ConfigDef()
            .define(PROPERTIES_INCLUDE_CONFIG, LIST, PROPERTIES_INCLUDE_DEFAULT, LOW, PROPERTIES_INCLUDE_DOC)
            .define(GROUPS_INCLUDE_CONFIG, LIST, GROUPS_INCLUDE_DEFAULT, LOW, GROUPS_INCLUDE_DOC)
            .define(ACL_INCLUDE_CONFIG, LIST, ACL_INCLUDE_DEFAULT, LOW, ACL_INCLUDE_DOC)
            .define(MIRROR_TOPIC_NUM_PARTITIONS_CONFIG, INT, MIRROR_TOPIC_NUM_PARTITIONS_DEFAULT, atLeast(1), HIGH, MIRROR_TOPIC_NUM_PARTITIONS_DOC)
            .define(MIRROR_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, MIRROR_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, MIRROR_TOPIC_REPLICATION_FACTOR_DOC)
            .define(NUM_REPLICA_FETCHERS_CONFIG, INT, NUM_REPLICA_FETCHERS_DEFAULT, atLeast(1), HIGH, NUM_REPLICA_FETCHERS_DOC)
            .define(METADATA_REFRESH_INTERVAL_MS_CONFIG, LONG, METADATA_REFRESH_INTERVAL_MS_DEFAULT, atLeast(0L), MEDIUM, METADATA_REFRESH_INTERVAL_MS_DOC);
    }

    /**
     * Represents an ACL include rule parsed from the semicolon-separated format:
     * resourceType;resourceName;operation;permissionType;principal
     *
     * Each field uses * as a wildcard (match all). The resourceName and principal
     * fields support regex patterns. Trailing wildcard fields can be omitted.
     *
     * Valid resourceType values: TOPIC, GROUP, CLUSTER, TRANSACTIONAL_ID, DELEGATION_TOKEN, USER.
     * Valid operation values: READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, CLUSTER_ACTION,
     * DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALL, etc.
     * Valid permissionType values: ALLOW, DENY.
     *
     * Examples:
     * <pre>
     * *                                       - match all ACLs (default)
     * TOPIC;orders.*                          - all ACLs for topics matching orders.*
     * *;*;*;*;User:alice                      - all ACLs for principal User:alice
     * *;*;*;*;User:app-.*                     - all ACLs for principals matching User:app-.*
     * TOPIC;*;READ;ALLOW                      - all topic READ/ALLOW ACLs
     * GROUP;consumer-.*;READ;ALLOW;User:bob   - READ/ALLOW ACLs on groups matching consumer-.* for User:bob
     * </pre>
     *
     * Config usage example:
     * <pre>
     * mirror.acl.include=TOPIC;orders.*, *;*;*;*;User:alice
     * </pre>
     *
     * @param resourceType the resource type to match, or null for wildcard
     * @param resourceNamePattern regex pattern for the resource name, or null for wildcard
     * @param operation the ACL operation to match, or null for wildcard
     * @param permissionType the ACL permission type to match, or null for wildcard
     * @param principalPattern regex pattern for the principal, or null for wildcard
     */
    public record AclRule(
            ResourceType resourceType,
            Pattern resourceNamePattern,
            AclOperation operation,
            AclPermissionType permissionType,
            Pattern principalPattern
    ) {
        /**
         * Parses a semicolon-separated rule string into an AclRule.
         *
         * @param rule the rule string (e.g., "TOPIC;orders.*;READ;ALLOW;User:alice")
         * @return the parsed AclRule
         */
        public static AclRule parse(String rule) {
            String[] parts = rule.trim().split(";", -1);

            ResourceType resourceType = null;
            Pattern resourceNamePattern = null;
            AclOperation operation = null;
            AclPermissionType permissionType = null;
            Pattern principalPattern = null;

            if (parts.length >= 1 && !"*".equals(parts[0].trim())) {
                resourceType = ResourceType.valueOf(parts[0].trim().toUpperCase(Locale.ROOT));
            }
            if (parts.length >= 2 && !"*".equals(parts[1].trim())) {
                resourceNamePattern = Pattern.compile(parts[1].trim());
            }
            if (parts.length >= 3 && !"*".equals(parts[2].trim())) {
                operation = AclOperation.valueOf(parts[2].trim().toUpperCase(Locale.ROOT));
            }
            if (parts.length >= 4 && !"*".equals(parts[3].trim())) {
                permissionType = AclPermissionType.valueOf(parts[3].trim().toUpperCase(Locale.ROOT));
            }
            if (parts.length >= 5 && !"*".equals(parts[4].trim())) {
                principalPattern = Pattern.compile(parts[4].trim());
            }

            return new AclRule(resourceType, resourceNamePattern, operation, permissionType, principalPattern);
        }

        /**
         * Tests whether the given AclBinding matches this rule.
         * A null field acts as a wildcard and matches any value.
         *
         * @param binding the ACL binding to test
         * @return true if the binding matches all non-wildcard fields of this rule
         */
        public boolean matches(AclBinding binding) {
            if (resourceType != null && binding.pattern().resourceType() != resourceType) {
                return false;
            }
            if (resourceNamePattern != null && !resourceNamePattern.matcher(binding.pattern().name()).matches()) {
                return false;
            }
            if (operation != null && binding.entry().operation() != operation) {
                return false;
            }
            if (permissionType != null && binding.entry().permissionType() != permissionType) {
                return false;
            }
            if (principalPattern != null && !principalPattern.matcher(binding.entry().principal()).matches()) {
                return false;
            }
            return true;
        }
    }
}
