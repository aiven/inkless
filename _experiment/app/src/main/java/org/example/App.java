package org.example;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class App {
    private static final String TOPIC = "t";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final String action = args[0];
        if (action.equals("create")) {
            createTopic();
        } else if (action.equals("consume")) {
            consume();
        } else {
            throw new RuntimeException("Unknown action " + action);
        }
    }

    private static void createTopic() throws ExecutionException, InterruptedException {
        final AdminClient adminClient = KafkaAdminClient.create(Map.of(
            "bootstrap.servers", "127.0.0.1:9092"
        ));

        try {
            final var r = adminClient.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)));
            r.all().get();
        } catch (final ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
            adminClient.deleteTopics(List.of(TOPIC)).all().get();
            final var r = adminClient.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)));
            r.all().get();
        }

        final KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
            "bootstrap.servers", "127.0.0.1:9092",
            "key.serializer", StringSerializer.class.getCanonicalName(),
            "value.serializer", StringSerializer.class.getCanonicalName()
        ));
        for (int i = 0; i < 100; i++) {
            final Future<RecordMetadata> send = producer.send(new ProducerRecord<>(TOPIC, 0, null, "value-" + i));
            producer.flush();
            send.get();
        }
    }

    private static void consume() {
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            "bootstrap.servers", "127.0.0.1:9092",
            "key.deserializer", StringDeserializer.class.getCanonicalName(),
            "value.deserializer", StringDeserializer.class.getCanonicalName(),
            "fetch.min.bytes", "1",
            "max.partition.fetch.bytes", "1",
            "fetch.max.bytes", "1",
            "max.poll.records", "1"
        ));
        final List<TopicPartition> tps = List.of(new TopicPartition(TOPIC, 0));
        consumer.assign(tps);
        consumer.seekToBeginning(tps);

        long offset = -1;
        while (offset < 99) {
            final ConsumerRecords<String, String> r = consumer.poll(Duration.ofMillis(100));
            for (final ConsumerRecord<String, String> record : r.records(TOPIC)) {
                offset = record.offset();
                System.out.println(record.value());
            }
        }
    }
}
