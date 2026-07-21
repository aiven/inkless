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
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

public class FindBatchesJob implements Supplier<Map<TopicIdPartition, FindBatchResponse>> {

    private final Time time;
    private final ControlPlane controlPlane;
    private final FetchParams params;
    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;
    private final int maxBatchesPerPartition;
    private final Consumer<Long> durationCallback;

    public FindBatchesJob(Time time,
                          ControlPlane controlPlane,
                          FetchParams params,
                          Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                          int maxBatchesPerPartition,
                          Consumer<Long> durationCallback) {
        this.time = time;
        this.controlPlane = controlPlane;
        this.params = params;
        this.fetchInfos = fetchInfos;
        this.maxBatchesPerPartition = maxBatchesPerPartition;
        this.durationCallback = durationCallback;
    }

    @Override
    public Map<TopicIdPartition, FindBatchResponse> get() {
        return TimeUtils.measureDurationMsSupplier(time, this::doWork, durationCallback);
    }

    private Map<TopicIdPartition, FindBatchResponse> doWork() {
        try {
            List<FindBatchRequest> requests = new ArrayList<>();
            for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> fetchInfo : fetchInfos.entrySet()) {
                TopicIdPartition topicIdPartition = fetchInfo.getKey();
                requests.add(new FindBatchRequest(topicIdPartition, fetchInfo.getValue().fetchOffset, fetchInfo.getValue().maxBytes));
            }

            List<FindBatchResponse> responses = controlPlane.findBatches(requests, params.maxBytes, maxBatchesPerPartition);

            // Validate that control plane returned the expected number of responses.
            // Control plane contract requires responses to be in the same order as requests:
            // responses[i] must correspond to requests[i] for all i.
            // This ensures index-based mapping is safe and partitions are mapped correctly.
            if (responses.size() != requests.size()) {
                throw new IllegalStateException(
                    "Control plane returned " + responses.size() + " responses but " + requests.size()
                        + " were requested. Responses must match requests in count and order."
                );
            }

            // Map responses to partitions by index, relying on control plane contract that
            // responses[i] corresponds to requests[i] for all i.
            Map<TopicIdPartition, FindBatchResponse> out = new HashMap<>();
            for (int i = 0; i < requests.size(); i++) {
                FindBatchRequest request = requests.get(i);
                FindBatchResponse response = responses.get(i);
                out.put(request.topicIdPartition(), response);
            }
            return out;
        } catch (Exception e) {
            throw new FindBatchesException(e);
        }
    }
}
