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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class FindBatchesJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456;

    static final String OBJECT_KEY = "obj1";
    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final String NON_EXISTENT_TOPIC = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);
    static final Uuid TOPIC_ID_2 = new Uuid(777, 888);
    static final Uuid NON_EXISTENT_TOPIC_ID = new Uuid(555, 333);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);
    static final TopicIdPartition T2P0 = new TopicIdPartition(TOPIC_ID_2, 0, TOPIC_2);
    static final TopicIdPartition T2P1 = new TopicIdPartition(TOPIC_ID_2, 1, TOPIC_2);
    static final TopicIdPartition T2P2 = new TopicIdPartition(TOPIC_ID_2, 2, TOPIC_2);
    static final TopicIdPartition T2P3 = new TopicIdPartition(TOPIC_ID_2, 3, TOPIC_2);
    static final List<TopicIdPartition> TOPIC_2_PARTITIONS = List.of(T2P0, T2P1, T2P2, T2P3);
    static final TopicIdPartition NON_EXISTENT_PARTITION = new TopicIdPartition(TOPIC_ID_0, 2, TOPIC_0);
    static final TopicIdPartition NON_EXISTENT_TOPIC_ID_PARTITION = new TopicIdPartition(NON_EXISTENT_TOPIC_ID, 2, NON_EXISTENT_TOPIC);

    Time time = new MockTime();

    @BeforeAll
    static void initDb() {
        pgContainer.createDatabase(FindBatchesJobTest.class.getSimpleName());
        pgContainer.migrate();
    }

    @AfterAll
    static void tearDownDb() {
        pgContainer.tearDown();
    }

    @BeforeEach
    void setUp() {
        final var jooqCtx = pgContainer.getJooqCtx();

        jooqCtx.transaction(ctx -> {
            ctx.dsl().query(
                """
                    TRUNCATE TABLE "logs", "batches", "files"
                    RESTART IDENTITY CASCADE;
                """
            ).execute();
        });

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 1),
            // Budget-fairness tests need more partitions than the shared budget can serve; kept on a
            // separate topic because partition 2 of TOPIC_0 is the NON_EXISTENT_PARTITION fixture.
            new CreateTopicAndPartitionsRequest(TOPIC_ID_2, TOPIC_2, 4)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests, duration -> {}).run();
    }

    @Test
    void topicIdPartitionNotFound() {
        final FindBatchesJob job = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(NON_EXISTENT_PARTITION, 0, 1000),
                new FindBatchRequest(NON_EXISTENT_TOPIC_ID_PARTITION, 0, 1000)
            ),
            2000,
            0,
            duration -> {}
        );
        final List<FindBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1, -1),
            new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1, -1)
        );
    }

    @Test
    void simpleFind() {
        commitBatches(OBJECT_KEY, List.of(T0P0), 1, 1234, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(new FindBatchRequest(T0P0, 0, 1500)),
            2000,
            0,
            duration -> {}
        ).call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1234, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 10)
        );
    }

    @Test
    void findCornerCases() {
        commitBatches(OBJECT_KEY, List.of(T0P0), 1, 1234, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                // This will produce a normal find result with some batches, even if the size is > max.partition.fetch.bytes.
                new FindBatchRequest(T0P0, 0, 1000),
                // This will be on the border, offset matching HWM, will produce no batches, but still a successful result.
                new FindBatchRequest(T0P1, 0, 1000),
                // This will result in the out-of-range error.
                new FindBatchRequest(T1P0, 10, 1000)
            ),
            2000,
            0,
            duration -> {}
        ).call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1234, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 10),
            new FindBatchResponse(Errors.NONE, List.of(), 0, 0),
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 0)
        );
    }

    @Test
    void twoRequestsAboutSamePartition() {
        commitBatches(OBJECT_KEY, List.of(T0P0), 2, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                // This should return both batches
                new FindBatchRequest(T0P0, 0, 100000),
                // This should return only the second batch
                new FindBatchRequest(T0P0, 13, 100000)
            ),
            3000,
            0,
            duration -> {}
        ).call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY, BatchMetadata.of(T0P0, 1000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 20),
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(2L, OBJECT_KEY, BatchMetadata.of(T0P0, 1000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 20)
        );
    }

    @DisplayName("Returns only the first batch when fetch.max.bytes <= first batch size")
    @ParameterizedTest(name = "fetchMaxBytes = {0}")
    @ValueSource(ints = {0, 1, 1000})
    void findWithFetchMaxBytesReturnsFirstBatch(int fetchMaxBytes) {
        commitBatches(OBJECT_KEY, List.of(T0P0), 2, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(new FindBatchRequest(T0P0, 0, Integer.MAX_VALUE)),
            fetchMaxBytes,
            0,
            duration -> {
            }
        ).call();

        assertThat(result).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 20)
        );
    }

    @DisplayName("Returns both batches when fetch.max.bytes > first batch size")
    @ParameterizedTest(name = "fetchMaxBytes = {0}")
    @ValueSource(ints = {1500, 2000, 999999})
    void findWithFetchMaxBytesReturnsBothBatches(int fetchMaxBytes) {
        commitBatches(OBJECT_KEY, List.of(T0P0), 2, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(new FindBatchRequest(T0P0, 0, Integer.MAX_VALUE)),
            fetchMaxBytes,
            0,
            duration -> {
            }
        ).call();

        assertThat(result).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY, BatchMetadata.of(T0P0, 1000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 20)
        );
    }

    @DisplayName("Returns only the first batch when max.partition.fetch.bytes <= first batch size")
    @ParameterizedTest(name = "maxPartitionFetchBytes = {0}")
    @ValueSource(ints = {1, 1000})
    void findWithMaxPartitionFetchBytesReturnsFirstBatch(int maxPartitionFetchBytes) {
        commitBatches(OBJECT_KEY, List.of(T0P0, T0P1), 2, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(T0P0, 0, maxPartitionFetchBytes),
                new FindBatchRequest(T0P1, 0, maxPartitionFetchBytes)
            ),
            Integer.MAX_VALUE,
            0,
            duration -> {}
        ).call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 20),
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(3L, OBJECT_KEY, BatchMetadata.of(T0P1, 2000, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 20)
        );
    }

    @DisplayName("Returns both batches when max.partition.fetch.bytes > first batch size")
    @ParameterizedTest(name = "maxPartitionFetchBytes = {0}")
    @ValueSource(ints = {1500, 2000, 999999})
    void findWithMaxPartitionFetchBytesReturnsBothBatches(int maxPartitionFetchBytes) {
        commitBatches(OBJECT_KEY, List.of(T0P0, T0P1), 2, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(T0P0, 0, maxPartitionFetchBytes),
                new FindBatchRequest(T0P1, 0, maxPartitionFetchBytes)
            ),
            Integer.MAX_VALUE,
            0,
            duration -> {}
        ).call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY, BatchMetadata.of(T0P0, 1000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 20),
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(3L, OBJECT_KEY, BatchMetadata.of(T0P1, 2000, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(4L, OBJECT_KEY, BatchMetadata.of(T0P1, 3000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 20)
        );
    }

    @ParameterizedTest(name = "maxPartitionFetchBytes = {0}, fetchMaxBytes = {1}")
    @CsvSource({
        "1000, 2000",
        "1000, 3000",
        "1000, 2147483647"
    })
    void findWithCombinedLimitsReturnsOneBatchPerPartition(int maxPartitionFetchBytes, int fetchMaxBytes) {
        // max.partition.fetch.bytes limits how many batches per partition, no matter the fetch.max.bytes value
        commitBatches(OBJECT_KEY, List.of(T0P0, T0P1), 10, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(T0P0, 0, maxPartitionFetchBytes),
                new FindBatchRequest(T0P1, 0, maxPartitionFetchBytes)
            ),
            fetchMaxBytes,
            0,
            duration -> {}
        ).call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100),
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(11L, OBJECT_KEY, BatchMetadata.of(T0P1, 10000, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100)
        );
    }

    @ParameterizedTest(name = "maxPartitionFetchBytes = {0}, fetchMaxBytes = {1}")
    @CsvSource({
        "1500, 2500",
        "2000, 3000"
    })
    void findWithCombinedLimitsReturnsTwoBatchesForFirstPartitionAndOneBatchForSecondPartition(int maxPartitionFetchBytes, int fetchMaxBytes) {
        // fetch.max.bytes limits the number of batches for second partition
        commitBatches(OBJECT_KEY, List.of(T0P0, T0P1), 10, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(T0P0, 0, maxPartitionFetchBytes),
                new FindBatchRequest(T0P1, 0, maxPartitionFetchBytes)
            ),
            fetchMaxBytes,
            0,
            duration -> {}
        ).call();

        // With the given limits, we expect both batches for T0P0, but only the first for T0P1 because of the overall fetchMaxBytes limit.
        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY, BatchMetadata.of(T0P0, 1000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100),
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(11L, OBJECT_KEY, BatchMetadata.of(T0P1, 10000, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100)
        );
    }

    @ParameterizedTest(name = "maxPartitionFetchBytes = {0}, fetchMaxBytes = {1}")
    @CsvSource({
        "1001, 4000",
        "1001, 4500",
        "1001, 100000",
        "2000, 4000",
        "2000, 4500",
        "2000, 100000",
    })
    void findWithCombinedLimitsReturnsTwoBatchesForBothPartitions(int maxPartitionFetchBytes, int fetchMaxBytes) {
        commitBatches(OBJECT_KEY, List.of(T0P0, T0P1), 10, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(T0P0, 0, maxPartitionFetchBytes),
                new FindBatchRequest(T0P1, 0, maxPartitionFetchBytes)
            ),
            fetchMaxBytes,
            0,
            duration -> {}
        ).call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY, BatchMetadata.of(T0P0, 1000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100),
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(11L, OBJECT_KEY, BatchMetadata.of(T0P1, 10000, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(12L, OBJECT_KEY, BatchMetadata.of(T0P1, 11000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100)
        );
    }

    @ParameterizedTest(name = "maxPartitionFetchBytes = {0}, fetchMaxBytes = {1}")
    @CsvSource({
        "1001, 4000",
        "1001, 4500",
        "1001, 100000",
        "2000, 4000",
        "2000, 4500",
        "2000, 100000",
    })
    void findWithCombinedLimitsReturnsOneBatchPerPartitionsAsLimit(int maxPartitionFetchBytes, int fetchMaxBytes) {
        commitBatches(OBJECT_KEY, List.of(T0P0, T0P1), 10, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(T0P0, 0, maxPartitionFetchBytes),
                new FindBatchRequest(T0P1, 0, maxPartitionFetchBytes)
            ),
            fetchMaxBytes,
            1,  // give the limit of 1 batch per partition
            duration -> {}
        ).call();

        // then we expect only one batch per partition, even if the limits would allow more
        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100),
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(11L, OBJECT_KEY, BatchMetadata.of(T0P1, 10000, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100)
        );
    }

    @ParameterizedTest(name = "maxPartitionFetchBytes = {0}, fetchMaxBytes = {1}")
    @CsvSource({
        "3001, 5001",
        "4000, 6000"
    })
    void findWithCombinedLimitsReturnsFourBatchesForFirstPartitionAndTwoForSecondPartition(int maxPartitionFetchBytes, int fetchMaxBytes) {
        commitBatches(OBJECT_KEY, List.of(T0P0, T0P1), 10, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(T0P0, 0, maxPartitionFetchBytes),
                new FindBatchRequest(T0P1, 0, maxPartitionFetchBytes)
            ),
            fetchMaxBytes,
            0,
            duration -> {}
        ).call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, OBJECT_KEY, BatchMetadata.of(T0P0, 0, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY, BatchMetadata.of(T0P0, 1000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(3L, OBJECT_KEY, BatchMetadata.of(T0P0, 2000, 1000, 20, 29, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(4L, OBJECT_KEY, BatchMetadata.of(T0P0, 3000, 1000, 30, 39, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100),
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(11L, OBJECT_KEY, BatchMetadata.of(T0P1, 10000, 1000, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                new BatchInfo(12L, OBJECT_KEY, BatchMetadata.of(T0P1, 11000, 1000, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
            ), 0, 100)
        );
    }

    // ---------------------------------------------------------------------------------------------
    // Cross-partition budget fairness.
    //
    // find_batches must behave like upstream ReplicaManager.readFromLocalLog: minOneMessage is a
    // once-per-*request* grant, cleared as soon as any partition returns bytes, after which the global
    // budget limits everyone else to zero. Partitions past the budget must return NO batches, because an
    // empty partition response (recordsSize == 0 / validBytes == 0) is the only signal that makes
    // FetchSession and AbstractFetcherThread leave a partition at the front of their rotation and serve it
    // first next round. A per-partition floor makes every partition look served, which pins the tail at
    // one batch per round.
    // ---------------------------------------------------------------------------------------------

    private List<FindBatchResponse> findWithBudget(
        final List<TopicIdPartition> partitions,
        final int maxPartitionFetchBytes,
        final int fetchMaxBytes
    ) {
        return new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            partitions.stream()
                .map(tp -> new FindBatchRequest(tp, 0, maxPartitionFetchBytes))
                .toList(),
            fetchMaxBytes,
            0,
            duration -> {
            }
        ).call();
    }

    private static List<Integer> batchCounts(final List<FindBatchResponse> responses) {
        return responses.stream().map(r -> r.batches().size()).toList();
    }

    /**
     * The rows removed from findWithMaxPartitionFetchBytesReturnsFirstBatch,
     * findWithCombinedLimitsReturnsOneBatchPerPartition and
     * findWithCombinedLimitsReturnsTwoBatchesForFirstPartitionAndOneBatchForSecondPartition, where the
     * request budget is spent on T0P0. They previously expected T0P1 to receive a floor batch.
     */
    @ParameterizedTest(name = "maxPartitionFetchBytes = {0}, fetchMaxBytes = {1} -> T0P0 gets {2}")
    @CsvSource({
        // The request-level grant covers T0P0's first batch; T0P1 is then out of budget.
        "0,          0,          1",
        "1,          1,          1",
        "1000,       1000,       1",
        // Per-partition cap admits nothing for T0P1 even though fetch.max.bytes is unlimited.
        "0,          2147483647, 1",
        // T0P0 takes two batches and exhausts fetch.max.bytes exactly.
        "1001,       2000,       2"
    })
    void findServesOnlyTheFirstPartitionWhenTheRequestBudgetIsSpent(
        final int maxPartitionFetchBytes,
        final int fetchMaxBytes,
        final int expectedFirstPartitionBatches
    ) {
        commitBatches(OBJECT_KEY, List.of(T0P0, T0P1), 10, 1000, 10);

        final List<FindBatchResponse> result = new FindBatchesJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new FindBatchRequest(T0P0, 0, maxPartitionFetchBytes),
                new FindBatchRequest(T0P1, 0, maxPartitionFetchBytes)
            ),
            fetchMaxBytes,
            0,
            duration -> {
            }
        ).call();

        final List<BatchInfo> firstPartitionBatches = new ArrayList<>();
        for (int i = 0; i < expectedFirstPartitionBatches; i++) {
            firstPartitionBatches.add(new BatchInfo(i + 1L, OBJECT_KEY, BatchMetadata.of(
                T0P0, i * 1000, 1000, i * 10L, i * 10L + 9,
                time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)));
        }

        // Responses stay in request order, so this asserts *which* partition was served, not just how many.
        assertThat(result).containsExactly(
            new FindBatchResponse(Errors.NONE, firstPartitionBatches, 0, 100),
            // Empty, not a floor batch: recordsSize == 0 is what leaves T0P1 at the front of the caller's
            // rotation so it is served first, with a full budget, on the next round.
            new FindBatchResponse(Errors.NONE, List.of(), 0, 100)
        );
    }

    @Test
    void findReturnsNoBatchesForPartitionsPastTheGlobalBudget() {
        commitBatches(OBJECT_KEY, TOPIC_2_PARTITIONS, 10, 1000, 10);

        // T2P0 alone can absorb the whole 8000-byte budget.
        final List<FindBatchResponse> result = findWithBudget(TOPIC_2_PARTITIONS, 8000, 8000);

        assertThat(batchCounts(result)).containsExactly(8, 0, 0, 0);
        // Empty is not an error, and the high watermark must still be reported so the consumer sees its lag.
        assertThat(result).allSatisfy(r -> {
            assertThat(r.errors()).isEqualTo(Errors.NONE);
            assertThat(r.highWatermark()).isEqualTo(100);
            assertThat(r.logStartOffset()).isEqualTo(0);
        });
    }

    @Test
    void findAlwaysReturnsAtLeastOneBatchForTheRequest() {
        commitBatches(OBJECT_KEY, TOPIC_2_PARTITIONS, 10, 1000, 10);

        // Budget far below a single 1000-byte batch: the request-level grant still serves the first
        // partition, so no fetch returns empty-handed while data exists.
        final List<FindBatchResponse> result = findWithBudget(TOPIC_2_PARTITIONS, 1, 1);

        assertThat(batchCounts(result)).containsExactly(1, 0, 0, 0);
    }

    /**
     * The request-level grant is consumed by the first partition that actually returns bytes, not by the
     * first partition in the request. A caught-up partition ahead of a lagging one must not burn it.
     *
     * This pins the choice of `l_global_bytes = 0` over a flag set on the first loop iteration: with a
     * budget below one batch, an implementation that granted only to the first *partition* would return
     * nothing at all for the whole request, which is the empty-handed fetch the grant exists to prevent.
     * It also matches upstream, where minOneMessage is cleared on the first non-empty read rather than
     * after the first partition is visited.
     */
    @Test
    void findCarriesTheRequestGrantPastPartitionsThatReturnNothing() {
        // Only T2P1 and T2P2 have data; T2P0 is an empty log, so a fetch at offset 0 is valid and empty.
        commitBatches(OBJECT_KEY, List.of(T2P1, T2P2), 10, 1000, 10);

        // fetchMaxBytes = 0 is what isolates the grant: it makes the budget clause
        // (l_global_bytes < fetch_max_bytes) false even at zero bytes consumed, so the only thing that can
        // admit T2P1's batch is the request-level grant. With any positive budget the clause would admit it
        // anyway and this test would pass without exercising the grant at all.
        final List<FindBatchResponse> result = findWithBudget(List.of(T2P0, T2P1, T2P2), 1000, 0);

        assertThat(batchCounts(result)).containsExactly(0, 1, 0);
        assertThat(result).allSatisfy(r -> assertThat(r.errors()).isEqualTo(Errors.NONE));
        // T2P0 is genuinely empty, not an error, and reports its own (zero) watermark.
        assertThat(result.get(0).highWatermark()).isEqualTo(0);
    }

    @Test
    void findOvershootsGlobalBudgetByAtMostOneBatch() {
        commitBatches(OBJECT_KEY, TOPIC_2_PARTITIONS, 10, 1000, 10);

        final int fetchMaxBytes = 2500;
        final List<FindBatchResponse> result = findWithBudget(TOPIC_2_PARTITIONS, 8000, fetchMaxBytes);

        final int totalBytes = result.stream()
            .flatMap(r -> r.batches().stream())
            .mapToInt(b -> (int) b.metadata().byteSize())
            .sum();
        // One batch of overshoot for the whole request, as upstream; not one per partition.
        assertThat(totalBytes).isLessThanOrEqualTo(fetchMaxBytes + 1000);
    }

    private void commitBatches(
        final String objectKey,
        final List<TopicIdPartition> topicPartitions,
        final int batchesPerPartition,
        final int batchSize,
        final int offsetsPerBatch
    ) {
        final List<CommitBatchRequest> requests = new ArrayList<>();
        int fileStartOffset = 0;
        final Map<TopicIdPartition, Long> kafkaBaseOffsets = new HashMap<>();

        for (final TopicIdPartition partition : topicPartitions) {
            for (int i = 0; i < batchesPerPartition; i++) {
                final long baseOffset = kafkaBaseOffsets.getOrDefault(partition, 0L);
                final long lastOffset = baseOffset + offsetsPerBatch - 1;

                requests.add(CommitBatchRequest.of(
                    0,
                    partition,
                    fileStartOffset,
                    batchSize,
                    baseOffset,
                    lastOffset,
                    time.milliseconds(),
                    TimestampType.CREATE_TIME
                ));

                fileStartOffset += batchSize;
                kafkaBaseOffsets.put(partition, lastOffset + 1);
            }
        }

        // Use the non-coalescing commit (commit_file_v1) so this fixture creates exactly one batches row
        // per request. These tests assert on per-batch counts/boundaries to exercise find_batches limit
        // logic; coalescing (the production default) would collapse contiguous same-partition rows and
        // change those counts. The row layout is what matters here, not how it was produced.
        final CommitFileJob commitJob = new CommitFileJob(
            time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID, FILE_SIZE, requests, false, duration -> {
        }
        );
        assertThat(commitJob.call()).isNotEmpty();
    }

}
