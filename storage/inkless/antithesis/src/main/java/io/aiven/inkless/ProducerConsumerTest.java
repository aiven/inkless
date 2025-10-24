/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless;

import com.antithesis.sdk.Lifecycle;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerConsumerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConsumerTest.class);

    public static void main(final String[] args) throws IOException, ExecutionException, InterruptedException {
        final String configFile = args[0];
        LOGGER.info("Using config file {}", configFile);

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final JsonNode config = mapper.readTree(new File(configFile));
        final String bootstrapServers = config.get("bootstrap_servers").asText();
        LOGGER.info("Bootstrap servers: {}", bootstrapServers);

        final String topicName = Uuid.randomUuid().toString();

        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            final NewTopic newTopic = new NewTopic(topicName, 1, (short) 1)
                .configs(Map.of("diskless.enable", "true"));
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            LOGGER.info("Created '{}' topic", topicName);
        }

        final Map<String, Object> producerProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
            ProducerConfig.ACKS_CONFIG, "-1"
        );
        try (final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 10; i++) {
                final byte[] key = ("key-" + i).getBytes();
                final byte[] value = ("Hello Kafka from byte array " + i).getBytes();
                
                final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, key, value);

                System.out.println(producer.send(record).get());
            }
        }

        Lifecycle.setupComplete(null);
    }
}
