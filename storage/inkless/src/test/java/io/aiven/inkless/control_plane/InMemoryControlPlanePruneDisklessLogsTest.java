/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryControlPlanePruneDisklessLogsTest {

    private static final int BROKER_ID = 7;
    private static final long FILE_MERGE_SIZE_THRESHOLD = 100 * 1024 * 1024;
    private static final Map<String, String> BASE_CONFIG = Map.of(
        "file.merge.size.threshold.bytes", Long.toString(FILE_MERGE_SIZE_THRESHOLD),
        "file.merge.lock.period.ms", "3600000"
    );

    static final String TOPIC = "topic-to-prune";
    static final Uuid TOPIC_ID = new Uuid(0xDEADBEEFL, 0xCAFEBABEL);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);
    static final Uuid UNKNOWN_TOPIC_ID = new Uuid(0xBADCAFEL, 0xFEEDFACE);

    private MockTime time;
    private InMemoryControlPlane cp;

    @BeforeEach
    void setUp() {
        time = new MockTime(0, 0, 0);
        cp = new InMemoryControlPlane(time);
        cp.configure(BASE_CONFIG);
        cp.createTopicAndPartitions(Set.of(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 1)));
    }

    @Test
    void emptyRequestListReturnsEmptyResponses() {
        assertThat(cp.pruneDisklessLogs(List.of())).isEmpty();
    }

    @Test
    void unknownPartitionReturnsErrorAndNullOffsetWithoutMutation() {
        final GetLogInfoResponse before = cp.getLogInfo(List.of(new GetLogInfoRequest(TOPIC_ID, 0))).get(0);
        assertThat(before.logStartOffset()).isEqualTo(0L);

        final TopicIdPartition requestTip = new TopicIdPartition(UNKNOWN_TOPIC_ID, new TopicPartition("x", 0));
        final List<PruneDisklessLogsResponse> responses = cp.pruneDisklessLogs(
            List.of(new PruneDisklessLogsRequest(requestTip, 0L)));

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.topicIdPartition()).isSameAs(requestTip);
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.UNKNOWN_TOPIC_OR_PARTITION);
                assertThat(r.disklessLogStartOffset()).isEqualTo(-1L);
            });

        final GetLogInfoResponse after = cp.getLogInfo(List.of(new GetLogInfoRequest(TOPIC_ID, 0))).get(0);
        assertThat(after.logStartOffset()).isEqualTo(before.logStartOffset());
    }

    @Test
    void doesNotDeleteBatchWhenHighestTieredIsStrictlyBelowBatchLastOffset() {
        final int batchBytes = 500;
        cp.commitFile(
            "obj-keep",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            batchBytes,
            List.of(CommitBatchRequest.of(0, T0P0, 0, batchBytes, 0L, 100L, 1000L, TimestampType.CREATE_TIME)));

        final List<PruneDisklessLogsResponse> responses = cp.pruneDisklessLogs(List.of(new PruneDisklessLogsRequest(T0P0, 99L)));

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.NONE);
                assertThat(r.disklessLogStartOffset()).isEqualTo(0L);
            });
        assertThat(cp.getLogInfo(List.of(new GetLogInfoRequest(TOPIC_ID, 0))).get(0).highWatermark()).isEqualTo(101L);
    }

    @Test
    void deletesBatchWhenLastOffsetEqualsHighestTieredAndNoBatchesRemain() {
        final int batchBytes = 400;
        cp.commitFile(
            "obj-all",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            batchBytes,
            List.of(CommitBatchRequest.of(0, T0P0, 0, batchBytes, 0L, 10L, 1000L, TimestampType.CREATE_TIME)));

        final List<PruneDisklessLogsResponse> responses = cp.pruneDisklessLogs(List.of(new PruneDisklessLogsRequest(T0P0, 10L)));

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.NONE);
                assertThat(r.disklessLogStartOffset()).isEqualTo(11L);
            });
        assertThat(cp.getFilesToDelete()).extracting(FileToDelete::objectKey).containsExactly("obj-all");
    }

    @Test
    void deletesOnlyFullyTieredBatchesAndSetsLogStartToMinBaseOfRemaining() {
        final int b1 = 300;
        final int b2 = 300;
        cp.commitFile(
            "obj-partial",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            b1 + b2,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, b1, 0L, 10L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P0, b1, b2, 11L, 25L, 1000L, TimestampType.CREATE_TIME)
            ));

        cp.pruneDisklessLogs(List.of(new PruneDisklessLogsRequest(T0P0, 10L)));

        assertThat(cp.getLogInfo(List.of(new GetLogInfoRequest(TOPIC_ID, 0))).get(0).logStartOffset()).isEqualTo(11L);
    }

    @Test
    void preservesLogStartWhenDeleteRecordsAdvancedIntoRemainingBatch() {
        final int b1 = 300;
        final int b2 = 300;
        cp.commitFile(
            "obj-delete-records-partial",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            b1 + b2,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, b1, 0L, 10L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P0, b1, b2, 11L, 25L, 1000L, TimestampType.CREATE_TIME)
            ));
        cp.deleteRecords(List.of(new DeleteRecordsRequest(T0P0, 5L)));

        final List<PruneDisklessLogsResponse> responses = cp.pruneDisklessLogs(List.of(new PruneDisklessLogsRequest(T0P0, 9L)));

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.NONE);
                assertThat(r.disklessLogStartOffset()).isEqualTo(5L);
            });
        assertThat(cp.getLogInfo(List.of(new GetLogInfoRequest(TOPIC_ID, 0))).get(0).logStartOffset()).isEqualTo(5L);
    }

    @Test
    void whenNoBatchesRemainDisklessStartIsMinOfHighWatermarkAndGreaterTieredFrontier() {
        final int batchBytes = 400;
        cp.commitFile(
            "obj-cap",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            batchBytes,
            List.of(CommitBatchRequest.of(0, T0P0, 0, batchBytes, 0L, 10L, 1000L, TimestampType.CREATE_TIME)));

        final long highWatermark = cp.getLogInfo(List.of(new GetLogInfoRequest(TOPIC_ID, 0))).get(0).highWatermark();
        assertThat(highWatermark).isEqualTo(11L);

        final List<PruneDisklessLogsResponse> responses = cp.pruneDisklessLogs(List.of(new PruneDisklessLogsRequest(T0P0, 100L)));

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.NONE);
                assertThat(r.disklessLogStartOffset()).isEqualTo(highWatermark);
            });
        assertThat(cp.getLogInfo(List.of(new GetLogInfoRequest(TOPIC_ID, 0))).get(0).logStartOffset()).isEqualTo(highWatermark);
    }

    @Test
    void responseUsesRequestTopicIdPartitionAsIsIncludingMismatchedTopicName() {
        final TopicIdPartition requestTip = new TopicIdPartition(TOPIC_ID, new TopicPartition("not-the-stored-name", 0));
        final int batchBytes = 400;
        cp.commitFile(
            "obj-as-is",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            batchBytes,
            List.of(CommitBatchRequest.of(0, T0P0, 0, batchBytes, 0L, 10L, 1000L, TimestampType.CREATE_TIME)));

        final List<PruneDisklessLogsResponse> responses = cp.pruneDisklessLogs(List.of(new PruneDisklessLogsRequest(requestTip, 10L)));

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.topicIdPartition()).isSameAs(requestTip);
                assertThat(r.topicIdPartition().topic()).isEqualTo("not-the-stored-name");
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.NONE);
                assertThat(r.disklessLogStartOffset()).isEqualTo(11L);
            });
    }

    @Test
    void twoRequestsSamePartitionAreAppliedSequentially() {
        final int b1 = 200;
        final int b2 = 200;
        cp.commitFile(
            "obj-seq",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            b1 + b2,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, b1, 0L, 5L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P0, b1, b2, 6L, 10L, 1000L, TimestampType.CREATE_TIME)
            ));

        final List<PruneDisklessLogsResponse> responses = cp.pruneDisklessLogs(List.of(
            new PruneDisklessLogsRequest(T0P0, 5L),
            new PruneDisklessLogsRequest(T0P0, 10L)
        ));

        assertThat(responses).hasSize(2);
        assertThat(responses.get(0).disklessLogStartOffset()).isEqualTo(6L);
        assertThat(responses.get(1).disklessLogStartOffset()).isEqualTo(11L);
    }

    @Test
    void twoPartitionsPreserveRequestOrder() {
        final Uuid topic2 = new Uuid(0x11111111L, 0x22222222L);
        final String name2 = "topic-2";
        cp.createTopicAndPartitions(Set.of(new CreateTopicAndPartitionsRequest(topic2, name2, 1)));
        final TopicIdPartition t2p0 = new TopicIdPartition(topic2, 0, name2);

        cp.commitFile(
            "a",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            200,
            List.of(CommitBatchRequest.of(0, T0P0, 0, 200, 0L, 5L, 1000L, TimestampType.CREATE_TIME)));
        cp.commitFile(
            "b",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            200,
            List.of(CommitBatchRequest.of(0, t2p0, 0, 200, 0L, 7L, 1000L, TimestampType.CREATE_TIME)));

        final TopicIdPartition req1 = new TopicIdPartition(t2p0.topicId(), new TopicPartition("alias-2", 0));
        final TopicIdPartition req0 = new TopicIdPartition(TOPIC_ID, new TopicPartition("alias-1", 0));

        final List<PruneDisklessLogsResponse> responses = cp.pruneDisklessLogs(List.of(
            new PruneDisklessLogsRequest(req1, 7L),
            new PruneDisklessLogsRequest(req0, 5L)
        ));

        assertThat(responses).hasSize(2);
        assertThat(responses.get(0).topicIdPartition()).isSameAs(req1);
        assertThat(responses.get(0).disklessLogStartOffset()).isEqualTo(8L);
        assertThat(responses.get(1).topicIdPartition()).isSameAs(req0);
        assertThat(responses.get(1).disklessLogStartOffset()).isEqualTo(6L);
    }
}
