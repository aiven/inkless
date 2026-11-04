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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.test.api.TestKitDefaults;
import org.apache.kafka.server.IntegrationTestUtils;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Sends single diskless produce, fetch, and list-offsets requests straight to one broker and
 * returns the partition error.
 *
 * <p>Clients retry {@code KAFKA_STORAGE_ERROR} because it is retriable, so they hide the error the
 * control-plane gate returns until their own timeout. The test also picks the broker that receives a raw
 * request, whichever broker the metadata names as the leader, and diskless produce accepts that.
 */
final class InklessDisklessRequestClient {

    private InklessDisklessRequestClient() {
    }

    static int brokerPort(final BrokerServer broker) {
        return broker.boundPort(ListenerName.normalised(TestKitDefaults.DEFAULT_BROKER_LISTENER_NAME));
    }

    static Errors produce(final int port, final Uuid topicId, final int partition) throws IOException {
        try (Socket socket = IntegrationTestUtils.connect(port)) {
            return produce(socket, topicId, partition);
        }
    }

    /** Sends the produce on an open connection, so repeated calls reuse it. */
    static Errors produce(final Socket socket, final Uuid topicId, final int partition) throws IOException {
        final ProduceRequest request = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(List.of(
                new ProduceRequestData.TopicProduceData()
                    .setTopicId(topicId)
                    .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                        .setIndex(partition)
                        .setRecords(MemoryRecords.withRecords(Compression.NONE,
                            new SimpleRecord("value".getBytes(StandardCharsets.UTF_8))))))).iterator()))
            .setAcks((short) 1)
            .setTimeoutMs(30_000)
            .setTransactionalId(null)).build();
        final ProduceResponse response = IntegrationTestUtils.sendAndReceive(request, socket);
        return Errors.forCode(response.data().responses().iterator().next()
            .partitionResponses().get(0).errorCode());
    }

    static Errors fetch(final int port, final String topic, final Uuid topicId, final int partition) throws IOException {
        final FetchRequest request = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 0, 1,
                Map.of(new TopicPartition(topic, partition),
                    new FetchRequest.PartitionData(topicId, 0, 0, 1024 * 1024, Optional.empty())))
            .build();
        final FetchResponse response = IntegrationTestUtils.connectAndReceive(request, port);
        return Errors.forCode(response.data().responses().get(0).partitions().get(0).errorCode());
    }

    static Errors listOffsets(final int port, final String topic, final int partition) throws IOException {
        final ListOffsetsRequest request = ListOffsetsRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(List.of(new ListOffsetsTopic()
                .setName(topic)
                .setPartitions(List.of(new ListOffsetsPartition()
                    .setPartitionIndex(partition)
                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)))))
            .setTimeoutMs(30_000)
            .build();
        final ListOffsetsResponse response = IntegrationTestUtils.connectAndReceive(request, port);
        return Errors.forCode(response.data().topics().get(0).partitions().get(0).errorCode());
    }
}
