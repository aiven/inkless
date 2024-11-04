package io.aiven.inkless.control_plane;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import io.aiven.inkless.common.BatchCoordinates;
import io.aiven.inkless.common.BatchInfo;
import io.aiven.inkless.common.CommitFileRequest;
import io.aiven.inkless.common.FindBatchRequest;
import io.aiven.inkless.common.FindBatchResponse;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestControlPlane {
    private static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    private static final TopicPartition T0P1 = new TopicPartition("topic0", 1);

    @Test
    public void testEmptyStorageEmptyRequest() {
        final ControlPlane controlPlane = new ControlPlane();
        final Map<TopicPartition, FindBatchResponse> result = controlPlane.findBatchesForFetch(List.of(), true, 100);
        assertThat(result).isEmpty();
    }

    @Test
    public void testNonEmptyStorageEmptyRequest() {
        final ControlPlane controlPlane = new ControlPlane();
        controlPlane.commitFile(new CommitFileRequest(
            "file1", List.of(
            new CommitFileRequest.Batch(T0P0, 0, 100, 10, 123)
        )
        ));

        final Map<TopicPartition, FindBatchResponse> result = controlPlane.findBatchesForFetch(List.of(), true, 100);
        assertThat(result).isEmpty();
    }

    @Test
    public void testEmptyStorageNonEmptyRequest() {
        final ControlPlane controlPlane = new ControlPlane();
        final Map<TopicPartition, FindBatchResponse> result = controlPlane.findBatchesForFetch(
            List.of(new FindBatchRequest(T0P0, 0, 1000)), true, 100);
        assertThat(result).isEqualTo(Map.of(T0P0, List.of()));
    }

    @Test
    public void testNonEmpty() {
        final ControlPlane controlPlane = new ControlPlane();
        controlPlane.commitFile(new CommitFileRequest(
            "file1", List.of(
                new CommitFileRequest.Batch(T0P0, 0, 100, 10, 0),
                new CommitFileRequest.Batch(T0P0, 100, 100, 10, 0),
                new CommitFileRequest.Batch(T0P1, 200, 200, 10, 0),
                new CommitFileRequest.Batch(T0P1, 400, 200, 10, 0)
            )
        ));
        controlPlane.commitFile(new CommitFileRequest(
            "file2", List.of(
                new CommitFileRequest.Batch(T0P1, 0, 10, 10, 0),
                new CommitFileRequest.Batch(T0P1, 10, 10, 10, 0),
                new CommitFileRequest.Batch(T0P0, 20, 20, 10, 0)
            )
        ));

        final Map<TopicPartition, FindBatchResponse> result =
            controlPlane.findBatchesForFetch(
                List.of(new FindBatchRequest(T0P0, 0, 200), new FindBatchRequest(T0P1, 12, 500)),
                true, 1000);
        assertThat(result).isEqualTo(Map.of(
            T0P0, List.of(
                new BatchInfo(new BatchCoordinates(T0P0, 0), "file1", 0, 100, 10),
                new BatchInfo(new BatchCoordinates(T0P0, 10), "file1", 100, 100, 10)
            ),
            T0P1, List.of(
                new BatchInfo(new BatchCoordinates(T0P1, 10), "file1", 400, 200, 10),
                new BatchInfo(new BatchCoordinates(T0P1, 20), "file2", 0, 10, 10),
                new BatchInfo(new BatchCoordinates(T0P1, 30), "file2", 10, 10, 10)
            )
        ));
    }
}
