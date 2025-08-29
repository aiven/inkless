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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
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
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FindBatchesJobTest {

    private final Time time = new MockTime();

    @Mock
    private ControlPlane controlPlane;
    @Mock
    private FetchParams params;

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
                new BatchInfo(1L, OBJECT_KEY_MAIN_PART, BatchMetadata.of(partition0, 0, 10, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );
        FindBatchesJob job = new FindBatchesJob(time, controlPlane, params, fetchInfos, durationMs -> {});
        when(controlPlane.findBatches(requestCaptor.capture(), anyInt())).thenReturn(new ArrayList<>(coordinates.values()));
        Map<TopicIdPartition, FindBatchResponse> result = job.call();

        assertThat(result).isEqualTo(coordinates);
    }

}
