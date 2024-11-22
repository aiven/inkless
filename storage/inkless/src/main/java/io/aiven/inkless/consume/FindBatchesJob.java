// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

public class FindBatchesJob implements Callable<Map<TopicIdPartition, FindBatchResponse>> {

    private final ControlPlane controlPlane;
    private final FetchParams params;
    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;

    public FindBatchesJob(ControlPlane controlPlane, FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
        this.controlPlane = controlPlane;
        this.params = params;
        this.fetchInfos = fetchInfos;
    }

    @Override
    public Map<TopicIdPartition, FindBatchResponse> call() throws Exception {
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
