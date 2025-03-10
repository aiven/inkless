/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

public class FindBatchesJob implements Callable<Map<TopicIdPartition, FindBatchResponse>> {

    private final Time time;
    private final ControlPlane controlPlane;
    private final FetchParams params;
    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;
    private final Consumer<Long> durationCallback;

    public FindBatchesJob(Time time,
                          ControlPlane controlPlane,
                          FetchParams params,
                          Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                          Consumer<Long> durationCallback) {
        this.time = time;
        this.controlPlane = controlPlane;
        this.params = params;
        this.fetchInfos = fetchInfos;
        this.durationCallback = durationCallback;
    }

    @Override
    public Map<TopicIdPartition, FindBatchResponse> call() throws Exception {
        return TimeUtils.measureDurationMs(time, this::doWork, durationCallback);
    }

    private Map<TopicIdPartition, FindBatchResponse> doWork() {
        List<FindBatchRequest> requests = new ArrayList<>();
        for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> fetchInfo : fetchInfos.entrySet()) {
            requests.add(new FindBatchRequest(fetchInfo.getKey(), fetchInfo.getValue().fetchOffset, fetchInfo.getValue().maxBytes));
        }

        List<FindBatchResponse> responses = controlPlane.findBatches(requests, params.maxBytes);

        Map<TopicIdPartition, FindBatchResponse> out = new HashMap<>();
        for (int i = 0; i < requests.size(); i++) {
            FindBatchRequest request = requests.get(i);
            FindBatchResponse response = responses.get(i);
            out.put(request.topicIdPartition(), response);
        }
        return out;
    }
}
