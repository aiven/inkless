// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FindBatchesJobTest {

    @Mock
    private Time time;
    @Mock
    private ControlPlane controlPlane;
    @Mock
    private FetchParams params;
    @Mock
    private Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;

    @Captor
    ArgumentCaptor<List<FindBatchRequest>> requestCaptor;

    Uuid topicId = Uuid.randomUuid();
    static final String OBJECT_KEY_MAIN_PART = "a";
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "inkless-topic");

    @Test
    public void findSingleBatch() throws Exception {
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                        BatchInfo.of(OBJECT_KEY_MAIN_PART, 0, 10, 0, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)
                ), logStartOffset, highWatermark)
        );
        FindBatchesJob job = new FindBatchesJob(time, controlPlane, params, fetchInfos, durationMs -> { });
        when(controlPlane.findBatches(requestCaptor.capture(), anyBoolean(), anyInt())).thenReturn(new ArrayList<>(coordinates.values()));
        Map<TopicIdPartition, FindBatchResponse> result = job.call();

        assertThat(result).isEqualTo(coordinates);
    }
}
