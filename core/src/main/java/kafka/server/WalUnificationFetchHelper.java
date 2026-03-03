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

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.consume.FetchHandler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Helper for the WAL unification fetcher: fetches from WAL (diskless object storage) via the
 * Control Plane and object storage, and returns partition data in the same shape as a fetch
 * response so the fetcher thread can append to the local log.
 */
public class WalUnificationFetchHelper {

    private final SharedState sharedState;
    private final FetchHandler fetchHandler;
    private final int maxBytes;
    private final long fetchTimeoutMs;

    public WalUnificationFetchHelper(SharedState sharedState, int maxBytes, long fetchTimeoutMs) {
        this.sharedState = sharedState;
        this.fetchHandler = new FetchHandler(sharedState);
        this.maxBytes = maxBytes;
        this.fetchTimeoutMs = fetchTimeoutMs;
    }

    /**
     * Fetch from WAL for the given partition fetch data (topicIdPartition -> fetch offset and limits).
     * Returns partition data keyed by TopicPartition for use with appendRecordsToFollowerOrFutureReplica.
     */
    public Map<TopicPartition, FetchResponseData.PartitionData> fetchFromWal(
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchData) {
        if (fetchData == null || fetchData.isEmpty()) {
            return Map.of();
        }

        FetchParams params = new FetchParams(
                sharedState.brokerId(),
                0L,
                fetchTimeoutMs,
                1,
                maxBytes,
                FetchIsolation.LOG_END,
                Optional.empty());

        try {
            Map<TopicIdPartition, FetchPartitionData> result = fetchHandler.handle(params, fetchData)
                    .get(fetchTimeoutMs + 1000, TimeUnit.MILLISECONDS);

            Map<TopicPartition, FetchResponseData.PartitionData> response = new HashMap<>();
            result.forEach((tidp, data) -> {
                TopicPartition tp = tidp.topicPartition();
                FetchResponseData.PartitionData pd = new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp.partition())
                        .setErrorCode(data.error.code())
                        .setHighWatermark(data.highWatermark)
                        .setLogStartOffset(data.logStartOffset)
                        .setRecords(data.records);
                response.put(tp, pd);
            });
            return response;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("WAL unification fetch interrupted", e);
        } catch (TimeoutException | ExecutionException e) {
            throw new RuntimeException("WAL unification fetch failed", e);
        }
    }

    public void close() {
        try {
            fetchHandler.close();
        } catch (Exception e) {
            throw new RuntimeException("Error closing WAL unification fetch helper", e);
        }
    }
}
