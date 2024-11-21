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

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.storage_backend.s3.S3StorageConfig;
import io.aiven.inkless.test_utils.S3TestContainer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class InklessClusterTest {

    private static final Logger log = LoggerFactory.getLogger(InklessClusterTest.class);

    @Container
    private static final LocalStackContainer LOCALSTACK = S3TestContainer.container();
    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup() throws Exception {
        final String bucketName = "inkless";
        try (S3Client s3Client = S3Client.builder()
            .region(Region.of(LOCALSTACK.getRegion()))
            .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())
                )
            )
            .build()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        }

        cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
                .setCombined(true)
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
                .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_BACKEND_CLASS_CONFIG, S3Storage.class.getName())
                .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName)
                .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_REGION_CONFIG, LOCALSTACK.getRegion())
                // Localstack
                .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_ENDPOINT_URL_CONFIG, LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString())
                .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG, "true")
                .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, LOCALSTACK.getAccessKey())
                .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, LOCALSTACK.getSecretKey())
                .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
    }

    @AfterEach
    public void teardown() throws Exception {
        cluster.close();
    }

    @Test
    public void createInklessTopic() throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        clientConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        clientConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        String topicName = "inkless-topic";
        byte numRecords = 10;

        try (Admin admin = AdminClient.create(clientConfigs)) {
            CreateTopicsResult topics = admin.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1)));
            topics.all().get(10, TimeUnit.SECONDS);
        }

        AtomicInteger recordsProduced = new AtomicInteger();
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(clientConfigs)) {
            for (byte i = 0; i < numRecords; i++) {
                byte[] value = new byte[]{i};
                producer.send(new ProducerRecord<>(topicName, value), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send record", exception);
                    } else {
                        log.info("Committed {} at offset {}", value, metadata.offset());
                        recordsProduced.incrementAndGet();
                    }
                });
            }
            producer.flush();
        }

        assertEquals(numRecords, recordsProduced.get());

        int recordsConsumed = 0;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(clientConfigs)) {
            consumer.assign(Collections.singletonList(new TopicPartition(topicName, 0)));
            ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofSeconds(30));
            for (ConsumerRecord<byte[], byte[]> record : poll) {
                log.info("Received record {} with data {}", recordsConsumed, record.value());
                recordsConsumed++;
            }
        }
        assertEquals(numRecords, recordsConsumed);
    }
}
