package io.aiven.inkless.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.common.TopicPartition;

import io.aiven.inkless.common.BatchCoordinates;
import io.aiven.inkless.common.BatchInfo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFetchPlanner {
    @Test
    public void testEmpty() {
        final FetchPlanner planner = new FetchPlanner();
        final List<FileFetch> plan = planner.plan(new HashMap<>());
        assertThat(plan).isEmpty();
    }

    @Test
    public void testSimple() {
        TopicPartition t0p0 = new TopicPartition("topic0", 0);

        // File 1:
        // t0p0b0 -- offset 0, 10 bytes
        final BatchInfo t0p0b0 = new BatchInfo(new BatchCoordinates(t0p0, 0), "file1", 0, 10, 1);
        // t0p0b1 -- offset 10, 14 bytes
        final BatchInfo t0p0b1 = new BatchInfo(new BatchCoordinates(t0p0, 0), "file1", 10, 14, 1);

        final HashMap<TopicPartition, List<BatchInfo>> planRequest = new HashMap<>();
        planRequest.put(t0p0, new ArrayList<>());
        planRequest.get(t0p0).add(t0p0b0);
        planRequest.get(t0p0).add(t0p0b1);

        final FetchPlanner planner = new FetchPlanner();
        final List<FileFetch> plan = planner.plan(planRequest);

        assertThat(plan).containsExactly(
            new FileFetch("file1", 0, 24, List.of(t0p0b0, t0p0b1))
        );
    }

    @Test
    public void testComplex() {
        TopicPartition t0p0 = new TopicPartition("topic0", 0);
        TopicPartition t0p1 = new TopicPartition("topic0", 1);
        TopicPartition t1p0 = new TopicPartition("topic1", 0);
        TopicPartition t1p1 = new TopicPartition("topic1", 1);

        // File 1:
        // t0p0b0 -- offset 0, 10 bytes
        final BatchInfo t0p0b0 = new BatchInfo(new BatchCoordinates(t0p0, 0), "file1", 0, 10, 1);
        // t0p1b0 -- offset 10, 11 bytes
        final BatchInfo t0p1b0 = new BatchInfo(new BatchCoordinates(t0p1, 0), "file1", 10, 11, 1);
        // t1p0b0 -- offset 21, 12 bytes
        final BatchInfo t1p0b0 = new BatchInfo(new BatchCoordinates(t1p0, 0), "file1", 21, 12, 1);
        // t1p1b0 -- offset 33, 13 bytes
        final BatchInfo t1p1b0 = new BatchInfo(new BatchCoordinates(t1p1, 0), "file1", 33, 13, 1);
        // t0p0b1 -- offset 46, 14 bytes
        final BatchInfo t0p0b1 = new BatchInfo(new BatchCoordinates(t0p0, 1), "file1", 46, 14, 1);
        // t0p1b1 -- offset 60, 15 bytes
        final BatchInfo t0p1b1 = new BatchInfo(new BatchCoordinates(t0p1, 1), "file1", 60, 15, 1);
        // t1p0b1 -- offset 75, 16 bytes
        final BatchInfo t1p0b1 = new BatchInfo(new BatchCoordinates(t1p0, 1), "file1", 75, 16, 1);
        // t1p1b1 -- offset 91, 17 bytes
        final BatchInfo t1p1b1 = new BatchInfo(new BatchCoordinates(t1p1, 1), "file1", 91, 17, 1);

        // File 2:
        // t1p1b3 -- offset 0, 27 bytes
        final BatchInfo t1p1b3 = new BatchInfo(new BatchCoordinates(t1p1, 2), "file2", 0, 27, 1);
        // t1p0b3 -- offset 27, 26 bytes
        final BatchInfo t1p0b3 = new BatchInfo(new BatchCoordinates(t1p0, 2), "file2", 27, 26, 1);
        // t0p1b3 -- offset 53, 25 bytes
        final BatchInfo t0p1b3 = new BatchInfo(new BatchCoordinates(t0p1, 2), "file2", 53, 25, 1);
        // t0p0b3 -- offset 78, 24 bytes
        final BatchInfo t0p0b3 = new BatchInfo(new BatchCoordinates(t0p0, 2), "file2", 78, 24, 1);
        // t1p1b2 -- offset 102, 23 bytes
        final BatchInfo t1p1b2 = new BatchInfo(new BatchCoordinates(t1p1, 3), "file2", 102, 23, 1);
        // t1p0b2 -- offset 125, 22 bytes
        final BatchInfo t1p0b2 = new BatchInfo(new BatchCoordinates(t1p0, 3), "file2", 125, 22, 1);
        // t0p1b2 -- offset 147, 21 bytes
        final BatchInfo t0p1b2 = new BatchInfo(new BatchCoordinates(t0p1, 3), "file2", 147, 21, 1);
        // t0p0b2 -- offset 168, 20 bytes
        final BatchInfo t0p0b2 = new BatchInfo(new BatchCoordinates(t0p0, 3), "file2", 168, 20, 1);

        final HashMap<TopicPartition, List<BatchInfo>> planRequest = new HashMap<>();
        planRequest.put(t0p0, new ArrayList<>());
        planRequest.get(t0p0).add(t0p0b0);
        planRequest.get(t0p0).add(t0p0b1);
        planRequest.get(t0p0).add(t0p0b2);
        planRequest.get(t0p0).add(t0p0b3);

        planRequest.put(t0p1, new ArrayList<>());
        planRequest.get(t0p1).add(t0p1b0);
        planRequest.get(t0p1).add(t0p1b1);
        planRequest.get(t0p1).add(t0p1b2);
        planRequest.get(t0p1).add(t0p1b3);

        planRequest.put(t1p1, new ArrayList<>());
        planRequest.get(t1p1).add(t1p1b0);
        planRequest.get(t1p1).add(t1p1b1);
        planRequest.get(t1p1).add(t1p1b2);
        planRequest.get(t1p1).add(t1p1b3);

        final FetchPlanner planner = new FetchPlanner();
        final List<FileFetch> plan = planner.plan(planRequest);

        assertThat(plan).containsExactlyInAnyOrder(
            new FileFetch("file1", 0, 10 + 11, List.of(t0p0b0, t0p1b0)),
            new FileFetch("file1", 33, 13 + 14 + 15, List.of(t1p1b0, t0p0b1, t0p1b1)),
            new FileFetch("file1", 91, 17, List.of(t1p1b1)),

            new FileFetch("file2", 0, 27, List.of(t1p1b3)),
            new FileFetch("file2", 53, 25 + 24 + 23, List.of(t0p1b3, t0p0b3, t1p1b2)),
            new FileFetch("file2", 147, 21 + 20, List.of(t0p1b2, t0p0b2))
        );
    }
}
