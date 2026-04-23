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
package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Test-only producer that supports three transaction modes:
 *   commit  - sends records, commits the transaction, then exits.
 *   abort   - sends records, aborts the transaction, then exits.
 *   pending - sends records, flushes, then exits without committing
 *             or aborting, leaving the transaction open on the broker.
 *
 * The --waiting-ms option adds a delay (in milliseconds) before
 * committing or aborting, allowing other producers to interleave
 * records in between.
 *
 * Non-transactional mode is used when --transactional-id is omitted.
 */
public class TransactionalTestProducer {

    public static void main(String[] args) throws Exception {
        Map<String, String> parsed = parseArgs(args);

        String bootstrapServers = parsed.get("bootstrap-server");
        String topic = parsed.get("topic");
        if (bootstrapServers == null || topic == null) {
            System.err.println("Usage: TransactionalTestProducer --bootstrap-server <servers> --topic <topic>"
                + " [--transactional-id <id>] [--mode commit|abort|pending] [--num-records <n>] [--waiting-ms <ms>]");
            Exit.exit(1);
        }

        String transactionalId = parsed.get("transactional-id");
        String mode = parsed.getOrDefault("mode", "commit");
        int numRecords = Integer.parseInt(parsed.getOrDefault("num-records", "1"));
        long waitingMs = Long.parseLong(parsed.getOrDefault("waiting-ms", "0"));

        produce(bootstrapServers, topic, transactionalId, mode, numRecords, waitingMs);
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parsed = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap-server":
                case "--topic":
                case "--transactional-id":
                case "--mode":
                case "--num-records":
                case "--waiting-ms":
                    parsed.put(args[i].substring(2), args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        return parsed;
    }

    static void produce(String bootstrapServers, String topic, String transactionalId,
                        String mode, int numRecords, long waitingMs) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        if (transactionalId != null) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000");
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            if (transactionalId != null) {
                producer.initTransactions();
                producer.beginTransaction();
            }

            for (int i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i));
            }
            producer.flush();

            if (transactionalId != null) {
                if (waitingMs > 0) {
                    Thread.sleep(waitingMs);
                }
                switch (mode) {
                    case "commit":
                        producer.commitTransaction();
                        break;
                    case "abort":
                        producer.abortTransaction();
                        break;
                    case "pending":
                        // halt to bypass producer.close() which would abort the open transaction
                        System.out.println("DONE");
                        System.out.flush();
                        Exit.halt(0);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown mode: " + mode);
                }
            }

            System.out.println("DONE");
        }
    }
}
