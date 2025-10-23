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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.BatchCoordinateCache;
import io.aiven.inkless.cache.LogFragment;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

public class FindBatchesJob implements Supplier<Map<TopicIdPartition, FindBatchResponse>> {

    private final Time time;
    private final ControlPlane controlPlane;
    private final BatchCoordinateCache batchCoordinateCache;
    private final FetchParams params;
    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;
    private final int maxBatchesPerPartition;
    private final Consumer<Long> durationCallback;

    private static final Logger LOGGER = LoggerFactory.getLogger(FindBatchesJob.class);

    public FindBatchesJob(Time time,
                          ControlPlane controlPlane,
                          BatchCoordinateCache batchCoordinateCache,
                          FetchParams params,
                          Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                          int maxBatchesPerPartition,
                          Consumer<Long> durationCallback) {
        this.time = time;
        this.controlPlane = controlPlane;
        this.batchCoordinateCache = batchCoordinateCache;
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

            FindBatchResponse[] orderedResponses = new FindBatchResponse[requests.size()];

            var controlPlaneRequests = new ArrayList<FindBatchRequest>();
            var controlPlaneIndices = new ArrayList<Integer>(); // To map responses back to their original slots

            for (int i = 0; i < requests.size(); i++) {
                FindBatchRequest request = requests.get(i);
                LogFragment logFragment = batchCoordinateCache.get(request.topicIdPartition(), request.offset());
                if (logFragment != null) {
                    // Cache hit: Place the response directly in its correct ordered slot.
                    orderedResponses[i] = FindBatchResponse.success(
                        logFragment.batches().stream().map(batchCoordinate -> batchCoordinate.batchInfo(1)).toList(),
                        logFragment.logStartOffset(),
                        logFragment.highWaterMark()
                    );
                } else {
                    // Cache miss: Add to the batch for the control plane and record its original index.
                    controlPlaneRequests.add(request);
                    controlPlaneIndices.add(i);
                }
            }

            List<FindBatchResponse> responses = controlPlane.findBatches(controlPlaneRequests, params.maxBytes, maxBatchesPerPartition);

            // Use the saved indices to place the control plane responses into their correct slots.
            for (int i = 0; i < controlPlaneRequests.size(); i++) {
                int originalIndex = controlPlaneIndices.get(i);
                FindBatchResponse response = responses.get(i);
                orderedResponses[originalIndex] = response;
            }

            Map<TopicIdPartition, FindBatchResponse> out = new LinkedHashMap<>();
            for (int i = 0; i < requests.size(); i++) {
                FindBatchRequest request = requests.get(i);
                FindBatchResponse response = orderedResponses[i];
                out.put(request.topicIdPartition(), response);
            }

            return out;

        } catch (Exception e) {
            throw new FindBatchesException(e);
        }
    }
}
