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
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fairness of the shared fetch byte budget across the partitions of one request, over several rounds.
 *
 * <p>Kafka does not divide a fetch budget between partitions. It spends the budget in request order and
 * relies on a <em>rotation across rounds</em> to take turns: a partition that returned bytes is moved to
 * the back of the caller's partition order, and one that returned nothing keeps its place and is served
 * first next round. Both consumers of this control-plane function implement that same rule:
 *
 * <ul>
 *   <li>consumer fetches - {@code FetchSession.PartitionIterator.hasNext} moves a partition to the end of
 *       {@code session.partitionMap} only when {@code FetchResponse.recordsSize(respData) > 0};
 *   <li>the consolidation fetcher - {@code AbstractFetcherThread.processFetchRequest} calls
 *       {@code partitionStates.updateAndMoveToEnd} only when {@code validBytes > 0}.
 * </ul>
 *
 * <p>So "returned zero bytes" is the only input to fairness, and a per-partition floor in
 * {@code find_batches_v2} defeats it: every partition looks served, every partition is moved to the back
 * in iteration order, relative order never changes, and the partitions after the budget is spent are
 * pinned at one batch per round while the head drains at full rate. Catch-up becomes serialized instead of
 * concurrent.
 *
 * <p>{@link #rotationHarness} replays that rule literally so the property can be asserted without a
 * broker: it is the composition of this function's budget behaviour with the caller's move-to-end rule
 * that is under test, not either half alone.
 */
@Testcontainers
class FindBatchesFairnessTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final String OBJECT_KEY = "obj1";
    static final String TOPIC = "topic";
    static final Uuid TOPIC_ID = new Uuid(31, 41);

    static final int OFFSETS_PER_BATCH = 10;

    Time time = new MockTime();

    @BeforeAll
    static void initDb() {
        pgContainer.createDatabase(FindBatchesFairnessTest.class.getSimpleName());
        pgContainer.migrate();
    }

    @AfterAll
    static void tearDownDb() {
        pgContainer.tearDown();
    }

    @BeforeEach
    void setUp() {
        pgContainer.getJooqCtx().transaction(ctx -> ctx.dsl().query(
            """
                TRUNCATE TABLE "logs", "batches", "files"
                RESTART IDENTITY CASCADE;
            """
        ).execute());
    }

    /**
     * Consumer fetch profile: 4 lagging partitions, a budget one partition can absorb on its own.
     *
     * <p>Over 4 rounds a fair allocation gives every partition one full-budget turn, so all four advance by
     * the same amount. Without the rotation the head partition takes the budget every round and the other
     * three are pinned at the one-batch floor, i.e. 8x the progress for the head.
     */
    @Test
    void budgetRotatesAcrossRoundsSoEveryPartitionAdvancesEqually() {
        final List<TopicIdPartition> partitions = createPartitions(4);
        commitBatches(partitions, 40, 1000);

        final Map<TopicIdPartition, Integer> served = rotationHarness(partitions, 1000 * 8, 1000 * 8, 4);

        assertThat(served.values()).as("batches served per partition over 4 rounds").containsOnly(8);
    }

    /**
     * Consolidation fetcher profile: more partitions than the aggregate budget can serve in one iteration.
     *
     * <p>Mirrors the shape of the shipped defaults, where
     * {@code diskless.consolidation.fetch.response.max.bytes} (64 MiB aggregate) is a small multiple of
     * {@code diskless.consolidation.fetch.max.bytes} (10 MiB per partition), so only a handful of the
     * partitions assigned to a fetcher can be served per iteration. The ratio is the property under test,
     * not the absolute byte counts: here the aggregate budget is exactly 5 per-partition budgets, so 5 of
     * the 10 partitions are served per iteration and the other 5 must wait their turn rather than each
     * taking a floor batch.
     *
     * <p>Over 10 rounds a fair allocation gives each partition 5 turns of 5 batches. Serialized allocation
     * leaves the first 5 draining at 5 batches/round while the rest are pinned at the one-batch floor.
     */
    @Test
    void consolidationBudgetRotatesSoAssignedPartitionsAdvanceEqually() {
        final int batchSize = 1024;
        final int perPartitionBudget = 5 * batchSize;
        final int aggregateBudget = 5 * perPartitionBudget;
        final List<TopicIdPartition> partitions = createPartitions(10);
        // Enough depth that no partition runs out: the serialized head takes 5 batches * 10 rounds.
        commitBatches(partitions, 60, batchSize);

        final Map<TopicIdPartition, Integer> served =
            rotationHarness(partitions, perPartitionBudget, aggregateBudget, 10);

        // 10 rounds * 5 partitions served per round / 10 partitions = 5 turns each, 5 batches per turn.
        assertThat(served.values()).as("batches served per partition over 10 rounds").containsOnly(25);
    }

    /**
     * Replay of the callers' move-to-end rotation rule against the real control-plane function.
     *
     * <p>Per round: issue one {@code find_batches} for the partitions in their current order, advance each
     * partition's fetch offset by what it was served, then move every partition that returned at least one
     * batch to the back of the order - which is exactly what {@code FetchSession} and
     * {@code AbstractFetcherThread} do, and the only fairness mechanism either has.
     *
     * @return batches served per partition, summed over all rounds
     */
    private Map<TopicIdPartition, Integer> rotationHarness(
        final List<TopicIdPartition> partitions,
        final int maxPartitionFetchBytes,
        final int fetchMaxBytes,
        final int rounds
    ) {
        final List<TopicIdPartition> order = new ArrayList<>(partitions);
        final Map<TopicIdPartition, Long> fetchOffsets = new HashMap<>();
        final Map<TopicIdPartition, Integer> servedBatches = new LinkedHashMap<>();
        partitions.forEach(tp -> {
            fetchOffsets.put(tp, 0L);
            servedBatches.put(tp, 0);
        });

        for (int round = 0; round < rounds; round++) {
            final List<FindBatchRequest> requests = order.stream()
                .map(tp -> new FindBatchRequest(tp, fetchOffsets.get(tp), maxPartitionFetchBytes))
                .toList();

            final List<FindBatchResponse> responses = new FindBatchesJob(
                time, pgContainer.getJooqCtx(), requests, fetchMaxBytes, 0, duration -> {
            }).call();

            final List<TopicIdPartition> returnedData = new ArrayList<>();
            for (int i = 0; i < requests.size(); i++) {
                final TopicIdPartition tp = requests.get(i).topicIdPartition();
                final FindBatchResponse response = responses.get(i);
                assertThat(response.errors()).isEqualTo(Errors.NONE);
                final int batches = response.batches().size();
                if (batches > 0) {
                    servedBatches.merge(tp, batches, Integer::sum);
                    fetchOffsets.merge(tp, (long) batches * OFFSETS_PER_BATCH, Long::sum);
                    returnedData.add(tp);
                }
            }

            // The callers' rule: bytes returned => move to the back; nothing returned => keep your place.
            order.removeAll(returnedData);
            order.addAll(returnedData);
        }
        return servedBatches;
    }

    private List<TopicIdPartition> createPartitions(final int count) {
        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, count)),
            duration -> {
            }
        ).run();
        final List<TopicIdPartition> partitions = new ArrayList<>(count);
        for (int p = 0; p < count; p++) {
            partitions.add(new TopicIdPartition(TOPIC_ID, p, TOPIC));
        }
        return partitions;
    }

    private void commitBatches(
        final List<TopicIdPartition> partitions,
        final int batchesPerPartition,
        final int batchSize
    ) {
        final List<CommitBatchRequest> requests = new ArrayList<>();
        int fileStartOffset = 0;
        for (final TopicIdPartition partition : partitions) {
            long baseOffset = 0;
            for (int i = 0; i < batchesPerPartition; i++) {
                final long lastOffset = baseOffset + OFFSETS_PER_BATCH - 1;
                requests.add(CommitBatchRequest.of(
                    0, partition, fileStartOffset, batchSize, baseOffset, lastOffset,
                    time.milliseconds(), TimestampType.CREATE_TIME
                ));
                fileStartOffset += batchSize;
                baseOffset = lastOffset + 1;
            }
        }
        // Non-coalescing commit (commit_file_v1) so the fixture yields exactly one batches row per request:
        // these tests count batches per partition, and coalescing would collapse contiguous same-partition
        // rows and change those counts.
        //
        // File size is the bytes actually written, not a constant: a short file would make every
        // CommitBatchRequest inconsistent with it. And assert per-response errors, because an all-errors
        // commit is still a non-empty list -- isNotEmpty() alone would let a broken fixture through and
        // resurface much later as "expected 25 batches but was 0", pointing at find_batches.
        assertThat(new CommitFileJob(
            time, pgContainer.getJooqCtx(), OBJECT_KEY, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID, fileStartOffset, requests, false, duration -> {
        }
        ).call())
            .isNotEmpty()
            .allSatisfy(r -> assertThat(r.errors()).isEqualTo(Errors.NONE));
    }
}
