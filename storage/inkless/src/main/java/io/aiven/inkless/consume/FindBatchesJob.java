// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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

        List<FindBatchResponse> responses = controlPlane.findBatches(requests, false, params.maxBytes);

        Map<TopicIdPartition, FindBatchResponse> out = new HashMap<>();
        for (int i = 0; i < requests.size(); i++) {
            FindBatchRequest request = requests.get(i);
            FindBatchResponse response = responses.get(i);
            out.put(request.topicIdPartition(), response);
        }
        return out;
    }
}
