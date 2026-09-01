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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.test.api.TestKitDefaults;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Sends a single Fetch request at a chosen API version straight to one broker's listener.
 *
 * <p>Exists to reach a code path no Kafka client can produce any more: the broker attaches client
 * metadata to a fetch only from Fetch v11 (`KafkaApis`), and without it `FetchParams.fetchOnlyLeader`
 * is true, so leader-only reads are enforced. Every current client negotiates v11 or later, which is
 * why a pre-v11 request has to be built by hand.
 *
 * <p>The wire framing mirrors `BaseRequestTest` on the Scala side; this exists because the diskless
 * cluster tests are Java on `KafkaClusterTestKit`, where that helper is not reachable.
 */
final class PreKip392FetchClient {

    private PreKip392FetchClient() {
    }

    /** What the caller needs from the response, so callers do not depend on the protocol classes. */
    record Reply(Errors error, int recordCount, long firstOffset, long highWatermark) {
    }

    /**
     * Fetches one partition from one broker and returns the single partition response.
     *
     * @param apiVersion Fetch API version to negotiate; below 11 the broker sees no client metadata
     */
    static Reply fetch(BrokerServer broker,
                       String topic,
                       int partition,
                       long fetchOffset,
                       short apiVersion) throws IOException {
        final int port = broker.boundPort(
            ListenerName.normalised(TestKitDefaults.DEFAULT_BROKER_LISTENER_NAME));
        final TopicPartition tp = new TopicPartition(topic, partition);
        final Map<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        // Uuid.ZERO_UUID: below Fetch v13 the request carries topic names, and the broker backfills
        // the topic id for diskless partitions.
        fetchData.put(tp, new FetchRequest.PartitionData(
            Uuid.ZERO_UUID, fetchOffset, 0L, 1024 * 1024, Optional.empty()));
        final FetchRequest request = FetchRequest.Builder
            .forConsumer(apiVersion, 1000, 1, fetchData)
            .build(apiVersion);
        final RequestHeader header = new RequestHeader(ApiKeys.FETCH, apiVersion, "pre-kip-392", 1);

        final FetchResponse response;
        try (Socket socket = new Socket("localhost", port)) {
            final byte[] serialized = Utils.toArray(request.serializeWithHeader(header));
            final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(serialized.length);
            out.write(serialized);
            out.flush();

            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final byte[] responseBytes = new byte[in.readInt()];
            in.readFully(responseBytes);
            final ByteBuffer buffer = ByteBuffer.wrap(responseBytes);
            ResponseHeader.parse(buffer, ApiKeys.FETCH.responseHeaderVersion(apiVersion));
            response = (FetchResponse) AbstractResponse.parseResponse(
                ApiKeys.FETCH, new ByteBufferAccessor(buffer), apiVersion);
        }

        final FetchResponseData.PartitionData partitionData =
            response.responseData(Map.of(), apiVersion).get(tp);
        if (partitionData == null) {
            throw new IllegalStateException("No partition data for " + tp + " in the fetch response");
        }
        final Errors error = Errors.forCode(partitionData.errorCode());
        int count = 0;
        long firstOffset = -1L;
        if (error == Errors.NONE) {
            for (final Record record : FetchResponse.recordsOrFail(partitionData).records()) {
                if (firstOffset < 0) {
                    firstOffset = record.offset();
                }
                count++;
            }
        }
        return new Reply(error, count, firstOffset, partitionData.highWatermark());
    }
}
