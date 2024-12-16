// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchPlannerTest {
    static final String OBJECT_KEY_PREFIX = "prefix/";
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = PlainObjectKey.creator(OBJECT_KEY_PREFIX);
    static final String OBJECT_KEY_A_MAIN_PART = "a";
    static final String OBJECT_KEY_B_MAIN_PART = "b";
    static final ObjectKey OBJECT_KEY_A = new PlainObjectKey(OBJECT_KEY_PREFIX, OBJECT_KEY_A_MAIN_PART);
    static final ObjectKey OBJECT_KEY_B = new PlainObjectKey(OBJECT_KEY_PREFIX, OBJECT_KEY_B_MAIN_PART);

    @Mock
    ObjectFetcher fetcher;
    @Mock
    ExecutorService dataExecutor;

    Time time = new MockTime();
    Uuid topicId = Uuid.randomUuid();
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "inkless-topic");
    TopicIdPartition partition1 = new TopicIdPartition(topicId, 1, "inkless-topic");

    @Test
    public void planEmptyRequest() throws Exception {
        Map<TopicIdPartition, FindBatchResponse> coordinates = new HashMap<>();
        FetchPlannerJob job = new FetchPlannerJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetcher,
            dataExecutor,
            CompletableFuture.completedFuture(coordinates),
            durationMs -> {},
            durationMs -> {}
        );

        job.call();

        verifyNoInteractions(dataExecutor);
    }

    @Test
    public void planSingleRequest() throws Exception {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_A_MAIN_PART, 0, 10, 0, 1, TimestampType.CREATE_TIME, 10, 20)
                ), 0, 1)
        ), Set.of(
                new FileFetchJob(
                    new MockTime(),
                    fetcher,
                    OBJECT_KEY_A,
                    new ByteRange(0, 10),
                    durationMs -> {}
                )
        ));
    }

    @Test
    public void planRequestsForMultipleObjects() throws Exception {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_A_MAIN_PART, 0, 10, 0, 1, TimestampType.CREATE_TIME, 10, 20),
                        new BatchInfo(OBJECT_KEY_B_MAIN_PART, 0, 10, 1, 1, TimestampType.CREATE_TIME, 11, 21)
                ), 0, 2)
        ), Set.of(
                new FileFetchJob(time, fetcher, OBJECT_KEY_A, new ByteRange(0, 10), durationMs -> {}),
                new FileFetchJob(time, fetcher, OBJECT_KEY_B, new ByteRange(0, 10), durationMs -> {})
        ));
    }

    @Test
    public void planRequestsForMultiplePartitions() throws Exception {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_A_MAIN_PART, 0, 10, 0, 1, TimestampType.CREATE_TIME, 10, 20)
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_B_MAIN_PART, 0, 10, 0, 1, TimestampType.CREATE_TIME, 11, 21)
                ), 0, 1)
        ), Set.of(
                new FileFetchJob(time, fetcher, OBJECT_KEY_A, new ByteRange(0, 10), durationMs -> {}),
                new FileFetchJob(time, fetcher, OBJECT_KEY_B, new ByteRange(0, 10), durationMs -> {})
        ));
    }

    @Test
    public void planMergedRequestsForSameObject() throws Exception {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_A_MAIN_PART, 0, 10, 0, 1, TimestampType.CREATE_TIME, 10, 20)
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_A_MAIN_PART, 30, 10, 0, 1, TimestampType.CREATE_TIME, 11, 21)
                ), 0,  1)
                ), Set.of(
                    new FileFetchJob(time, fetcher, OBJECT_KEY_A, new ByteRange(0, 40), durationMs -> {})
        ));
    }

    @Test
    public void planOffsetOutOfRange() throws Exception {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.offsetOutOfRange(0, 1),
                partition1, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_B_MAIN_PART, 0, 10, 0, 1, TimestampType.CREATE_TIME, 11, 21)
                ), 0, 1)
        ), Set.of(
                new FileFetchJob(time, fetcher, OBJECT_KEY_B, new ByteRange(0, 10), durationMs -> {})
        ));
    }

    @Test
    public void planUnknownTopicOrPartition() throws Exception {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.unknownTopicOrPartition(),
                partition1, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_B_MAIN_PART, 0, 10, 0, 1, TimestampType.CREATE_TIME, 11, 21)
                ), 0, 1)
        ), Set.of(
                new FileFetchJob(time, fetcher, OBJECT_KEY_B, new ByteRange(0, 10), durationMs -> {})
        ));
    }

    @Test
    public void planUnknownServerError() throws Exception {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.unknownServerError(),
                partition1, FindBatchResponse.success(List.of(
                        new BatchInfo(OBJECT_KEY_B_MAIN_PART, 0, 10, 0, 1, TimestampType.CREATE_TIME, 11, 21)
                ), 0, 1)
        ), Set.of(
                new FileFetchJob(time, fetcher, OBJECT_KEY_B, new ByteRange(0, 10), durationMs -> {})
        ));
    }

    private void assertBatchPlan(Map<TopicIdPartition, FindBatchResponse> coordinates, Set<FileFetchJob> jobs) throws Exception {
        ArgumentCaptor<FileFetchJob> submittedCallables = ArgumentCaptor.captor();
        when(dataExecutor.submit(submittedCallables.capture())).thenReturn(null);

        FetchPlannerJob job = new FetchPlannerJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetcher,
            dataExecutor,
            CompletableFuture.completedFuture(coordinates),
            durationMs -> {},
            durationMs -> {}
        );

        job.call();

        assertEquals(jobs, new HashSet<>(submittedCallables.getAllValues()));
    }

}
