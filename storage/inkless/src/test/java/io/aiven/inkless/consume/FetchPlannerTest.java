// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchPlannerTest {

    @Mock
    ObjectFetcher fetcher;
    @Mock
    ExecutorService dataExecutor;
    Uuid topicId = Uuid.randomUuid();
    ObjectKey objectA = new PlainObjectKey("a", "a");
    ObjectKey objectB = new PlainObjectKey("b", "b");
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "inkless-topic");
    TopicIdPartition partition1 = new TopicIdPartition(topicId, 1, "inkless-topic");

    @Test
    public void planEmptyRequest() {
        Map<TopicIdPartition, FindBatchResponse> coordinates = new HashMap<>();
        FetchPlannerJob job = new FetchPlannerJob(fetcher, dataExecutor, CompletableFuture.completedFuture(coordinates));

        job.call();

        verifyNoInteractions(dataExecutor);
    }

    @Test
    public void planSingleRequest() {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(objectA, 0, 10, 0, 1)
                ), 0, 1)
        ), Set.of(
                new FileFetchJob(fetcher, objectA, new ByteRange(0, 10))
        ));
    }

    @Test
    public void planRequestsForMultipleObjects() {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(objectA, 0, 10, 0, 1),
                        new BatchInfo(objectB, 0, 10, 1, 1)
                ), 0, 2)
        ), Set.of(
                new FileFetchJob(fetcher, objectA, new ByteRange(0, 10)),
                new FileFetchJob(fetcher, objectB, new ByteRange(0, 10))
        ));
    }

    @Test
    public void planRequestsForMultiplePartitions() {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(objectA, 0, 10, 0, 1)
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                        new BatchInfo(objectB, 0, 10, 0, 1)
                ), 0, 1)
        ), Set.of(
                new FileFetchJob(fetcher, objectA, new ByteRange(0, 10)),
                new FileFetchJob(fetcher, objectB, new ByteRange(0, 10))
        ));
    }

    @Test
    public void planMergedRequestsForSameObject() {
        assertBatchPlan(Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(objectA, 0, 10, 0, 1)
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                        new BatchInfo(objectA, 30, 10, 0, 1)
                ), 0, 1)
                ), Set.of(
                new FileFetchJob(fetcher, objectA, new ByteRange(0, 40))
        ));
    }

    private void assertBatchPlan(Map<TopicIdPartition, FindBatchResponse> coordinates, Set<FileFetchJob> jobs) {
        ArgumentCaptor<FileFetchJob> submittedCallables = ArgumentCaptor.captor();
        when(dataExecutor.submit(submittedCallables.capture())).thenReturn(null);

        FetchPlannerJob job = new FetchPlannerJob(fetcher, dataExecutor, CompletableFuture.completedFuture(coordinates));

        job.call();

        assertEquals(jobs, new HashSet<>(submittedCallables.getAllValues()));
    }

}
