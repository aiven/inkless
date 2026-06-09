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
package kafka.server.mirror;

import kafka.server.KafkaConfig;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MirrorMetadataManagerTest {
    private Properties initialProps;

    @BeforeEach
    public void beforeEach() {
        initialProps = new Properties();
        initialProps.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        initialProps.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1");
        initialProps.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        initialProps.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
        initialProps.setProperty(
                BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                String.format("%s,%s", PlainSaslServer.PLAIN_MECHANISM, ScramMechanism.SCRAM_SHA_256.mechanismName())
        );
        initialProps.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "INTERNAL");
        initialProps.setProperty(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, ScramMechanism.SCRAM_SHA_256.mechanismName());
        initialProps.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT");
        initialProps.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "INTERNAL://:9093");

        String prefix = "listener.name.internal.";
        initialProps.setProperty(prefix + ScramMechanism.SCRAM_SHA_256.mechanismName().toLowerCase(Locale.ROOT) + "." + SaslConfigs.SASL_JAAS_CONFIG, "jaasConfig");
        initialProps.setProperty(prefix + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore.jks");
    }

    @Test
    public void testParseMirrorAdminPropsDefaultsToInterBrokerProtocol() {
        // Given
        KafkaConfig config = new KafkaConfig(initialProps);

        // When
        Properties parsedConfig = MirrorMetadataManager.buildDestAdminClientProps(config);

        // Then
        assertEquals(ScramMechanism.SCRAM_SHA_256.mechanismName(), parsedConfig.get(SaslConfigs.SASL_MECHANISM));
        assertEquals("SASL_PLAINTEXT", parsedConfig.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG));
        assertEquals("jaasConfig", parsedConfig.get(SaslConfigs.SASL_JAAS_CONFIG));
        assertEquals("/path/to/keystore.jks", parsedConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    }

    @Test
    public void testParseMirrorAdminPropsParsesMirrorListenerWhenConfiguredExplicitly() {
        // Given
        initialProps.setProperty(ReplicationConfigs.MIRROR_ADMIN_LISTENER_NAME_CONFIG, "MIRROR");
        initialProps.setProperty(BrokerSecurityConfigs.SASL_MECHANISM_MIRROR_ADMIN_PROTOCOL_CONFIG, PlainSaslServer.PLAIN_MECHANISM);
        initialProps.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,MIRROR:SASL_SSL");
        initialProps.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "INTERNAL://:9093,MIRROR://:9094");

        String prefix = "listener.name.mirror.";
        initialProps.setProperty(prefix + PlainSaslServer.PLAIN_MECHANISM.toLowerCase(Locale.ROOT) + "." + SaslConfigs.SASL_JAAS_CONFIG, "jaasConfigMirror");
        initialProps.setProperty(prefix + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore-mirror.jks");
        KafkaConfig config = new KafkaConfig(initialProps);

        // When
        Properties parsedConfig = MirrorMetadataManager.buildDestAdminClientProps(config);

        // Then
        assertEquals(PlainSaslServer.PLAIN_MECHANISM, parsedConfig.get(SaslConfigs.SASL_MECHANISM));
        assertEquals("SASL_SSL", parsedConfig.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG));
        assertEquals("jaasConfigMirror", parsedConfig.get(SaslConfigs.SASL_JAAS_CONFIG));
        assertEquals("/path/to/keystore-mirror.jks", parsedConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    }
}
