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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchPlannerTest {
    static final String OBJECT_KEY_PREFIX = "prefix";
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator(OBJECT_KEY_PREFIX, false);
    static final String OBJECT_KEY_A_MAIN_PART = "a";
    static final String OBJECT_KEY_B_MAIN_PART = "b";
    static final ObjectKey OBJECT_KEY_A = PlainObjectKey.create(OBJECT_KEY_PREFIX, OBJECT_KEY_A_MAIN_PART);
    static final ObjectKey OBJECT_KEY_B = PlainObjectKey.create(OBJECT_KEY_PREFIX, OBJECT_KEY_B_MAIN_PART);

    @Mock
    ObjectFetcher fetcher;
    @Mock
    ExecutorService dataExecutor;
    @Mock
    InklessFetchMetrics metrics;

    ObjectCache cache = new NullCache();
    KeyAlignmentStrategy keyAlignmentStrategy = new FixedBlockAlignment(Integer.MAX_VALUE);
    ByteRange requestRange = new ByteRange(0, Integer.MAX_VALUE);
    Time time = new MockTime();
    Uuid topicId = Uuid.randomUuid();
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "diskless-topic");
    TopicIdPartition partition1 = new TopicIdPartition(topicId, 1, "diskless-topic");

    @Test
    public void planEmptyRequest() {
        Map<TopicIdPartition, FindBatchResponse> coordinates = new HashMap<>();
        FetchPlanner job = fetchPlannerJob(coordinates);

        job.get();

        verifyNoInteractions(dataExecutor);
    }

    @Test
    public void planSingleRequest() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                cacheFetchJob(OBJECT_KEY_A, requestRange)
            )
        );
    }

    @Test
    public void planRequestsForMultipleObjects() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME)),
                    new BatchInfo(2L, OBJECT_KEY_B.value(), BatchMetadata.of(partition0, 0, 10, 1, 1, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 2)
            ),
            Set.of(
                cacheFetchJob(OBJECT_KEY_A, requestRange),
                cacheFetchJob(OBJECT_KEY_B, requestRange)
            )
        );
    }

    @Test
    public void planRequestsForMultiplePartitions() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(2L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                cacheFetchJob(OBJECT_KEY_A, requestRange),
                cacheFetchJob(OBJECT_KEY_B, requestRange)
            )
        );
    }

    @Test
    public void planMergedRequestsForSameObject() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(2L, OBJECT_KEY_A.value(), BatchMetadata.of(partition1, 30, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0,  1)
            ),
            Set.of(
                cacheFetchJob(OBJECT_KEY_A, requestRange)
            )
        );
    }

    @Test
    public void planOffsetOutOfRange() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.offsetOutOfRange(0, 1),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                cacheFetchJob(OBJECT_KEY_B, requestRange)
            )
        );
    }

    @Test
    public void planUnknownTopicOrPartition() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.unknownTopicOrPartition(),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1,0, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                cacheFetchJob(OBJECT_KEY_B, requestRange)
            )
        );
    }

    @Test
    public void planUnknownServerError() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.unknownServerError(),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                cacheFetchJob(OBJECT_KEY_B, requestRange)
            )
        );
    }

    private FetchPlanner fetchPlannerJob(Map<TopicIdPartition, FindBatchResponse> batchCoordinatesFuture) {
        return new FetchPlanner(
            time, FetchPlannerTest.OBJECT_KEY_CREATOR, keyAlignmentStrategy,
            cache, fetcher, dataExecutor, batchCoordinatesFuture, metrics
        );
    }

    private CacheFetchJob cacheFetchJob(
        ObjectKey objectKey,
        ByteRange byteRange
    ) {
        return new CacheFetchJob(
            cache,
            fetcher,
            objectKey,
            byteRange,
            time,
            durationMs -> {},
            durationMs -> {},
            hitBool -> {},
            durationMs -> {},
            cacheEntrySize -> {}
        );
    }

    private void assertBatchPlan(Map<TopicIdPartition, FindBatchResponse> coordinates, Set<CacheFetchJob> jobs) {
        ArgumentCaptor<CacheFetchJob> submittedCallables = ArgumentCaptor.captor();
        when(dataExecutor.submit(submittedCallables.capture())).thenReturn(null);

        FetchPlanner job = fetchPlannerJob(coordinates);

        job.get();

        assertEquals(jobs, new HashSet<>(submittedCallables.getAllValues()));
    }
}
