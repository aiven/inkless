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

import com.antithesis.sdk.Assert;
import com.antithesis.sdk.Lifecycle;
import com.antithesis.sdk.Random;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ProducerConsumerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConsumerTest.class);

    public static void main(final String[] args) throws IOException, ExecutionException, InterruptedException {
        final String configFile = args[0];
        LOGGER.info("Using config file {}", configFile);

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final JsonNode config = mapper.readTree(new File(configFile));
        final String bootstrapServers = config.get("bootstrap_servers").asText();
        LOGGER.info("Bootstrap servers: {}", bootstrapServers);

        final List<String> topics = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            topics.add(Uuid.randomUuid().toString());
        }

        final List<TopicPartition> topicPartitions = new ArrayList<>();

        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            final List<NewTopic> newTopics = new ArrayList<>();
            for (int i = 0; i < topics.size(); i++) {
                final String topic = topics.get(i);
                final int numPartitions = i + 1;
                newTopics.add(new NewTopic(topic, numPartitions, (short) 1)
                    .configs(Map.of("diskless.enable", "true")));

                for (int j = 0; j < numPartitions; j++) {
                    topicPartitions.add(new TopicPartition(topic, j));
                }
            }
            adminClient.createTopics(newTopics).all().get();
            LOGGER.info("Created topics: {}", newTopics);
        }

        Lifecycle.setupComplete(null);

        final ConcurrentHashMap<TopicPartition, ConcurrentHashMap<Long, ProducerRecord<byte[], byte[]>>> sentRecords =
            new ConcurrentHashMap<>();
        final AtomicBoolean errors = new AtomicBoolean(false);

        final List<ProducerThread> producerThreads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            producerThreads.add(new ProducerThread(1, bootstrapServers, topicPartitions, sentRecords, errors));
        }
        producerThreads.forEach(Thread::start);

        final long waitMs = 60_000;
        final long started = System.currentTimeMillis();
        for (final ProducerThread t : producerThreads) {
            final long toWait = waitMs - (System.currentTimeMillis() - started);
            if (toWait > 0) {
                t.join(toWait);
            }
            Assert.always(!t.isAlive(), "Producer threads finish in time", null);
            if (t.isAlive()) {
                LOGGER.error("Producer thread {} is still running, aborting", t);
                return;
            }
        }
        Assert.reachable("All threads finished", null);

        Assert.always(!errors.get(), "Produce without errors", null);
        if (errors.get()) {
            LOGGER.error("Produce errors detected, aborting");
            return;
        }

        // Consume records and verify them
        consumeAndVerifyRecords(bootstrapServers, topicPartitions, sentRecords);
    }

    private static void consumeAndVerifyRecords(final String bootstrapServers,
                                                final List<TopicPartition> topicPartitions,
                                                final ConcurrentHashMap<TopicPartition, ConcurrentHashMap<Long, ProducerRecord<byte[], byte[]>>> sentRecords
    ) throws ExecutionException, InterruptedException {
        LOGGER.info("Verifying records");

        final Map<String, Object> consumerProps = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.CLIENT_ID_CONFIG, "verification-consumer",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
            ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes(),
            ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes()
        );
        final var consumer = new KafkaConsumer<byte[], byte[]>(consumerProps);

        final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
        for (final var entry : beginningOffsets.entrySet()) {
            final Long beginningOffset = entry.getValue();
            Assert.always(beginningOffset == 0, "Beginning offset as expected",
                new ObjectNode(JsonNodeFactory.instance)
                    .put("topicPartition", entry.getKey().topic())
                    .put("beginningOffset", beginningOffset)
            );
            if (beginningOffset != 0) {
                LOGGER.error("Beginning offset: {}, expected 0 in {}", beginningOffset, topicPartitions);
                return;
            }
        }

        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        final Map<TopicPartition, Long> expectedEndOffsets = new HashMap<>();
        for (final var entry : endOffsets.entrySet()) {
            final Long endOffset = entry.getValue();
            final TopicPartition tp = entry.getKey();
            final Long expectedEndOffset = sentRecords.get(tp).keySet().stream().max(Long::compare).get() + 1;
            expectedEndOffsets.put(tp, expectedEndOffset);
            Assert.always(Objects.equals(endOffset, expectedEndOffset), "End offset as expected",
                new ObjectNode(JsonNodeFactory.instance)
                    .put("topicPartition", tp.topic())
                    .put("endOffset", endOffset)
                    .put("expectedEndOffset", expectedEndOffset)
            );
            if (!Objects.equals(endOffset, expectedEndOffset)) {
                LOGGER.error("End offset: {}, expected {} in {}", endOffset, expectedEndOffset, topicPartitions);
                return;
            }
        }

        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);
        final Map<TopicPartition, Long> currentOffsets = new HashMap<>(topicPartitions.stream()
            .collect(Collectors.toMap(tp -> tp, tp -> 0L)));
        while (shouldConsumeMore(currentOffsets, expectedEndOffsets)) {
            final ConsumerRecords<byte[], byte[]> pollResult = consumer.poll(Duration.ofMillis(pollDuration()));
            for (final TopicPartition tp : pollResult.partitions()) {
                for (final var record : pollResult.records(tp)) {
                    final long currentOffset = currentOffsets.get(tp);
                    Assert.always(record.offset() == currentOffset, "Offsets go in order",
                        new ObjectNode(JsonNodeFactory.instance)
                            .put("currentOffset", currentOffset)
                            .put("record.offset()", record.offset())
                    );
                    if (record.offset() != currentOffset) {
                        LOGGER.error("Current offset: {}, record offset: {}", currentOffset, record.offset());
                        return;
                    }

                    final ProducerRecord<byte[], byte[]> sentRecord = sentRecords.get(tp).get(currentOffset);
                    Assert.always(sentRecord != null, "Records for all offsets exist",
                        new ObjectNode(JsonNodeFactory.instance)
                            .put("currentOffset", currentOffset)
                    );
                    if (sentRecord == null) {
                        LOGGER.error("No set record for offset {}", sentRecord);
                        return;
                    }

                    final boolean keysMatch = Arrays.equals(sentRecord.key(), record.key());
                    Assert.always(keysMatch, "Record keys match",
                        new ObjectNode(JsonNodeFactory.instance)
                            .put("currentOffset", currentOffset)
                    );
                    if (!keysMatch) {
                        LOGGER.error("Keys don't match at offset {}", currentOffset);
                        return;
                    }

                    currentOffsets.put(tp, currentOffset + 1);
                }
            }
        }

        LOGGER.info("All records verified");
        Assert.reachable("All records verified", null);
    }

    private static boolean shouldConsumeMore(final Map<TopicPartition, Long> currentOffsets,
                                             final Map<TopicPartition, Long> expectedEndOffsets) {
        if (!currentOffsets.keySet().equals(expectedEndOffsets.keySet())) {
            throw new RuntimeException();
        }
        for (final TopicPartition tp : currentOffsets.keySet()) {
            if (currentOffsets.get(tp) < expectedEndOffsets.get(tp) - 1) {
                return true;
            }
        }
        return false;
    }

    private static int maxPartitionFetchBytes() {
        return Math.abs((int) (Random.getRandom() % 1024 * 1024));
    }

    private static int fetchMaxBytes() {
        return Math.abs((int) (Random.getRandom() % 50 * 1024 * 1024));
    }

    private static int pollDuration() {
        return Math.abs((int) (Random.getRandom() % 200));
    }

    private static class ProducerThread extends Thread {
        private final int threadId;
        private final List<TopicPartition> topicPartitions;
        private final ConcurrentHashMap<TopicPartition, ConcurrentHashMap<Long, ProducerRecord<byte[], byte[]>>> sentRecords;
        private final AtomicBoolean errors;
        private final KafkaProducer<byte[], byte[]> producer;

        private ProducerThread(final int threadId,
                               final String bootstrapServers,
                               final List<TopicPartition> topicPartitions,
                               final ConcurrentHashMap<TopicPartition, ConcurrentHashMap<Long, ProducerRecord<byte[], byte[]>>> sentRecords,
                               final AtomicBoolean errors) {
            super("producer-" + threadId);
            this.threadId = threadId;
            this.topicPartitions = topicPartitions;
            this.sentRecords = sentRecords;
            this.errors = errors;

            final Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, String.format("producer-%d", threadId),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG, "-1",
                ProducerConfig.LINGER_MS_CONFIG, lingerMs()
            );
            this.producer = new KafkaProducer<>(producerProps);
        }

        @Override
        public void run() {
            final int numRecords = 1000;
            LOGGER.info("Producing {} records to each partition", numRecords);
            for (int i = 0; i < numRecords; i++) {
                for (final TopicPartition tp : topicPartitions) {
                    final byte[] key = (String.format("key-%s-%d-%d", tp, threadId, i)).getBytes();
                    final byte[] value = (String.format("value-%s-%d-%d", tp, threadId, i).repeat(100)).getBytes();
                    final ProducerRecord<byte[], byte[]> record =
                        new ProducerRecord<>(tp.topic(), tp.partition(), key, value);

                    producer.send(record, (rm, e) -> {
                        if (e == null) {
                            sentRecords.computeIfAbsent(tp, _unused -> new ConcurrentHashMap<>());
                            sentRecords.get(tp).putIfAbsent(rm.offset(), record);
                        } else {
                            LOGGER.error("Error sending", e);
                            errors.set(true);
                        }
                    });
                }

                try {
                    Thread.sleep(interRecordDelay());
                } catch (final InterruptedException e) {
                    LOGGER.warn("Thread {} interrupted", this);
                    throw new RuntimeException(e);
                }
            }
            producer.flush();
        }

        private static int interRecordDelay() {
            return Math.abs((int) (Random.getRandom() % 50));
        }

        private static long lingerMs() {
            return Math.abs(Random.getRandom() % 1000L);
        }
    }
}
