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
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetRequest;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.GetProducerStateRequest;
import io.aiven.inkless.control_plane.GetProducerStateResponse;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP;
import static org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP;
import static org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PostgresControlPlaneStandbyProtectionTest {
    private static final int BROKER_ID = 1;
    private static final long FILE_SIZE = 1024L;
    private static final String TOPIC = "standby-protection-topic";
    private static final Uuid TOPIC_ID = new Uuid(101, 202);
    private static final TopicIdPartition TP0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);
    private static final TopicIdPartition TP1 = new TopicIdPartition(TOPIC_ID, 1, TOPIC);

    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    private final MockTime time = new MockTime(0, 10_000L, 0);
    private final List<PostgresControlPlane> openedControlPlanes = new ArrayList<>();

    private String primaryDbUrl;
    private String standbyDbUrl;
    private PostgresControlPlane primaryControlPlane;
    private PostgresControlPlane standbyControlPlane;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        final String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        primaryDbUrl = createDatabase("primary_" + suffix);
        standbyDbUrl = createDatabase("standby_" + suffix);

        primaryControlPlane = openSingleDbControlPlane(primaryDbUrl);
        standbyControlPlane = openSingleDbControlPlane(standbyDbUrl);

        final Set<CreateTopicAndPartitionsRequest> topics = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 2)
        );
        primaryControlPlane.createTopicAndPartitions(topics);
        standbyControlPlane.createTopicAndPartitions(topics);

        // Both databases have offsets [0..9] on TP0 and [0..4] on TP1.
        commitBatch(primaryControlPlane, "obj-0", TP0, 0, 9, 1_000L);
        commitBatch(standbyControlPlane, "obj-0", TP0, 0, 9, 1_000L);
        commitBatch(primaryControlPlane, "obj-p1", TP1, 0, 4, 1_500L);
        commitBatch(standbyControlPlane, "obj-p1", TP1, 0, 4, 1_500L);

        // Primary advances ahead of standby on TP0 with offsets [10..19].
        commitBatch(primaryControlPlane, "obj-1", TP0, 10, 19, 2_000L);
    }

    @AfterEach
    void tearDown() throws IOException {
        for (final PostgresControlPlane cp : openedControlPlanes) {
            cp.close();
        }
        openedControlPlanes.clear();
    }

    @Test
    void aboveStandbyHwmConfirmedOnPrimaryReturnsBatchesAndUpdatesMetrics() {
        final PostgresControlPlane cp = openStandbyProtectedControlPlane(primaryDbUrl, standbyDbUrl, primaryDbUrl, Map.of());
        assertThat(cp.standbyProtectionEnabled()).isTrue();

        final long confirmBefore = meterCount("FindBatchesPrimaryConfirmRate");
        final long overrideBefore = meterCount("FindBatchesPrimaryOverrideRate");
        final long failureBefore = meterCount("FindBatchesPrimaryConfirmFailureRate");
        final long rejectedBefore = meterCount("PrimaryReadLimitRejectedRate");
        final long primaryQueryCountBefore = histogramCount("FindBatchesPrimaryQueryTime");

        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(new FindBatchRequest(TP0, 12, 1024 * 1024)),
            1024 * 1024,
            0
        );

        assertThat(responses).hasSize(1);
        final FindBatchResponse response = responses.get(0);
        assertThat(response.errors()).isEqualTo(Errors.NONE);
        assertThat(response.highWatermark()).isEqualTo(20L);
        assertThat(response.batches()).hasSize(1);
        assertThat(response.batches().get(0).objectKey()).isEqualTo("obj-1");
        assertThat(response.batches().get(0).metadata().baseOffset()).isEqualTo(10L);
        assertThat(response.batches().get(0).metadata().lastOffset()).isEqualTo(19L);

        assertThat(meterCount("FindBatchesPrimaryConfirmRate")).isEqualTo(confirmBefore + 1);
        assertThat(meterCount("FindBatchesPrimaryOverrideRate")).isEqualTo(overrideBefore + 1);
        assertThat(meterCount("FindBatchesPrimaryConfirmFailureRate")).isEqualTo(failureBefore);
        assertThat(meterCount("PrimaryReadLimitRejectedRate")).isEqualTo(rejectedBefore);
        assertThat(histogramCount("FindBatchesPrimaryQueryTime")).isEqualTo(primaryQueryCountBefore + 1);
    }

    @Test
    void genuineAbovePrimaryHwmStaysOffsetOutOfRangeWithoutOverride() {
        final PostgresControlPlane cp = openStandbyProtectedControlPlane(primaryDbUrl, standbyDbUrl, primaryDbUrl, Map.of());

        final long confirmBefore = meterCount("FindBatchesPrimaryConfirmRate");
        final long overrideBefore = meterCount("FindBatchesPrimaryOverrideRate");
        final long primaryQueryCountBefore = histogramCount("FindBatchesPrimaryQueryTime");

        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(new FindBatchRequest(TP0, 25, 1024 * 1024)),
            1024 * 1024,
            0
        );

        assertThat(responses).hasSize(1);
        final FindBatchResponse response = responses.get(0);
        assertThat(response.errors()).isEqualTo(Errors.OFFSET_OUT_OF_RANGE);
        assertThat(response.highWatermark()).isEqualTo(20L);

        assertThat(meterCount("FindBatchesPrimaryConfirmRate")).isEqualTo(confirmBefore + 1);
        assertThat(meterCount("FindBatchesPrimaryOverrideRate")).isEqualTo(overrideBefore);
        assertThat(histogramCount("FindBatchesPrimaryQueryTime")).isEqualTo(primaryQueryCountBefore + 1);
    }

    @Test
    void belowLogStartOffsetSkipsPrimaryConfirmation() {
        primaryControlPlane.deleteRecords(List.of(new DeleteRecordsRequest(TP0, 5L)));
        standbyControlPlane.deleteRecords(List.of(new DeleteRecordsRequest(TP0, 5L)));

        final PostgresControlPlane cp = openStandbyProtectedControlPlane(primaryDbUrl, standbyDbUrl, primaryDbUrl, Map.of());

        final long confirmBefore = meterCount("FindBatchesPrimaryConfirmRate");
        final long overrideBefore = meterCount("FindBatchesPrimaryOverrideRate");
        final long primaryQueryCountBefore = histogramCount("FindBatchesPrimaryQueryTime");

        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(new FindBatchRequest(TP0, 2, 1024 * 1024)),
            1024 * 1024,
            0
        );

        assertThat(responses).hasSize(1);
        final FindBatchResponse response = responses.get(0);
        assertThat(response.errors()).isEqualTo(Errors.OFFSET_OUT_OF_RANGE);
        assertThat(response.logStartOffset()).isEqualTo(5L);
        assertThat(response.highWatermark()).isEqualTo(10L);

        assertThat(meterCount("FindBatchesPrimaryConfirmRate")).isEqualTo(confirmBefore);
        assertThat(meterCount("FindBatchesPrimaryOverrideRate")).isEqualTo(overrideBefore);
        assertThat(histogramCount("FindBatchesPrimaryQueryTime")).isEqualTo(primaryQueryCountBefore);
    }

    @Test
    void mixedMultiPartitionReturnsKafkaStorageErrorWhenConcurrencyLimitIsReached() throws InterruptedException {
        final PostgresControlPlane cp = openStandbyProtectedControlPlane(
            primaryDbUrl,
            standbyDbUrl,
            primaryDbUrl,
            Map.of(PostgresControlPlaneConfig.STANDBY_FALLBACK_MAX_CONCURRENT_CONFIG, "1")
        );

        final long confirmBefore = meterCount("FindBatchesPrimaryConfirmRate");
        final long overrideBefore = meterCount("FindBatchesPrimaryOverrideRate");
        final long failureBefore = meterCount("FindBatchesPrimaryConfirmFailureRate");
        final long rejectedBefore = meterCount("PrimaryReadLimitRejectedRate");

        cp.primaryReadLimiter().acquire();
        try {
            final List<FindBatchResponse> responses = cp.findBatches(
                List.of(
                    new FindBatchRequest(TP1, 0, 1024 * 1024),
                    new FindBatchRequest(TP0, 12, 1024 * 1024)
                ),
                1024 * 1024,
                0
            );

            assertThat(responses).hasSize(2);
            assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
            assertThat(responses.get(0).batches()).hasSize(1);
            assertThat(responses.get(0).batches().get(0).objectKey()).isEqualTo("obj-p1");

            assertThat(responses.get(1)).isEqualTo(FindBatchResponse.kafkaStorageError());
        } finally {
            cp.primaryReadLimiter().release();
        }

        assertThat(meterCount("FindBatchesPrimaryConfirmRate")).isEqualTo(confirmBefore + 1);
        assertThat(meterCount("FindBatchesPrimaryOverrideRate")).isEqualTo(overrideBefore);
        assertThat(meterCount("FindBatchesPrimaryConfirmFailureRate")).isEqualTo(failureBefore + 1);
        assertThat(meterCount("PrimaryReadLimitRejectedRate")).isEqualTo(rejectedBefore + 1);
    }

    @Test
    void mixedMultiPartitionReturnsKafkaStorageErrorWhenPrimaryConfirmationFails() {
        // An unmigrated empty database accepts JDBC connections during `configure()` and fails on `find_batches_v2`.
        final String brokenPrimaryDbUrl = createDatabase("broken_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12));
        final PostgresControlPlane cp = openStandbyProtectedControlPlane(primaryDbUrl, standbyDbUrl, brokenPrimaryDbUrl, Map.of());

        final long confirmBefore = meterCount("FindBatchesPrimaryConfirmRate");
        final long overrideBefore = meterCount("FindBatchesPrimaryOverrideRate");
        final long failureBefore = meterCount("FindBatchesPrimaryConfirmFailureRate");
        final long rejectedBefore = meterCount("PrimaryReadLimitRejectedRate");

        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(
                new FindBatchRequest(TP1, 0, 1024 * 1024),
                new FindBatchRequest(TP0, 12, 1024 * 1024)
            ),
            1024 * 1024,
            0
        );

        assertThat(responses).hasSize(2);
        assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(0).batches()).hasSize(1);
        assertThat(responses.get(0).batches().get(0).objectKey()).isEqualTo("obj-p1");

        assertThat(responses.get(1)).isEqualTo(FindBatchResponse.kafkaStorageError());

        assertThat(meterCount("FindBatchesPrimaryConfirmRate")).isEqualTo(confirmBefore + 1);
        assertThat(meterCount("FindBatchesPrimaryOverrideRate")).isEqualTo(overrideBefore);
        assertThat(meterCount("FindBatchesPrimaryConfirmFailureRate")).isEqualTo(failureBefore + 1);
        assertThat(meterCount("PrimaryReadLimitRejectedRate")).isEqualTo(rejectedBefore);
    }

    @Test
    void listOffsetsCrossTierLogStartAndProducerStateReadFromPrimary() {
        // Advance primary state beyond standby for deleteRecords, cross-tier log start, and producer state.
        primaryControlPlane.deleteRecords(List.of(new DeleteRecordsRequest(TP0, 4L)));
        primaryControlPlane.advanceCrossTierLogStartOffset(List.of(
            new AdvanceCrossTierLogStartOffsetRequest(TOPIC_ID, 0, 7L)
        ));
        final long producerId = 99L;
        final short producerEpoch = 1;
        primaryControlPlane.commitFile(
            "obj-idempotent",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            FILE_SIZE,
            List.of(CommitBatchRequest.idempotent(
                0, TP0, 0, 128, 0, 0, 3_000L, TimestampType.CREATE_TIME, producerId, producerEpoch, 0, 0
            ))
        );

        final PostgresControlPlane cp = openStandbyProtectedControlPlane(primaryDbUrl, standbyDbUrl, primaryDbUrl, Map.of());

        final List<ListOffsetsResponse> listResponses = cp.listOffsets(List.of(
            new ListOffsetsRequest(TP0, LATEST_TIMESTAMP),
            new ListOffsetsRequest(TP0, EARLIEST_TIMESTAMP),
            new ListOffsetsRequest(TP0, EARLIEST_LOCAL_TIMESTAMP),
            new ListOffsetsRequest(TP0, 2_500L)
        ));

        assertThat(listResponses).hasSize(4);
        // Primary has WAL `log_start_offset` 4, cross-tier log start 7, and the idempotent batch at offset 20 (ts 3000).
        assertThat(listResponses.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(listResponses.get(0).offset()).isEqualTo(21L);
        assertThat(listResponses.get(1).errors()).isEqualTo(Errors.NONE);
        assertThat(listResponses.get(1).offset()).isEqualTo(7L);
        assertThat(listResponses.get(2).errors()).isEqualTo(Errors.NONE);
        assertThat(listResponses.get(2).offset()).isEqualTo(4L);
        assertThat(listResponses.get(3).errors()).isEqualTo(Errors.NONE);
        assertThat(listResponses.get(3).offset()).isEqualTo(20L);

        assertThat(cp.getCrossTierLogStart(TP0)).isEqualTo(OptionalLong.of(7L));

        final List<GetProducerStateResponse> producerStates = cp.getProducerState(List.of(
            new GetProducerStateRequest(TOPIC_ID, 0)
        ));
        assertThat(producerStates).hasSize(1);
        assertThat(producerStates.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(producerStates.get(0).entries())
            .extracting(GetProducerStateResponse.ProducerStateEntry::producerId)
            .containsExactly(producerId);
    }

    @Test
    void readOnlyWithoutReadPrimaryDoesNotFallBackToJobsDataSource() {
        final Map<String, String> configs = new HashMap<>();
        configs.put("connection.string", primaryDbUrl);
        configs.put("username", pgContainer.getUsername());
        configs.put("password", pgContainer.getPassword());
        configs.put("read.connection.string", standbyDbUrl);
        configs.put("read.username", pgContainer.getUsername());
        configs.put("read.password", pgContainer.getPassword());

        final PostgresControlPlane cp = openControlPlane(configs);
        assertThat(cp.standbyProtectionEnabled()).isFalse();

        final long confirmBefore = meterCount("FindBatchesPrimaryConfirmRate");

        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(new FindBatchRequest(TP0, 12, 1024 * 1024)),
            1024 * 1024,
            0
        );
        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).errors()).isEqualTo(Errors.OFFSET_OUT_OF_RANGE);
        assertThat(responses.get(0).highWatermark()).isEqualTo(10L);
        assertThat(meterCount("FindBatchesPrimaryConfirmRate")).isEqualTo(confirmBefore);

        final List<ListOffsetsResponse> listResponses = cp.listOffsets(List.of(
            new ListOffsetsRequest(TP0, LATEST_TIMESTAMP)
        ));
        assertThat(listResponses).hasSize(1);
        assertThat(listResponses.get(0).offset()).isEqualTo(10L);
    }

    @Test
    void readPrimaryOnlyRoutesReadsToReadPrimaryWithoutStandbyProtection() {
        final Map<String, String> configs = new HashMap<>();
        configs.put("connection.string", standbyDbUrl);
        configs.put("username", pgContainer.getUsername());
        configs.put("password", pgContainer.getPassword());
        configs.put("read.primary.connection.string", primaryDbUrl);
        configs.put("read.primary.username", pgContainer.getUsername());
        configs.put("read.primary.password", pgContainer.getPassword());

        final PostgresControlPlane cp = openControlPlane(configs);
        assertThat(cp.standbyProtectionEnabled()).isFalse();

        final long confirmBefore = meterCount("FindBatchesPrimaryConfirmRate");
        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(new FindBatchRequest(TP0, 12, 1024 * 1024)),
            1024 * 1024,
            0
        );
        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(0).highWatermark()).isEqualTo(20L);
        assertThat(meterCount("FindBatchesPrimaryConfirmRate")).isEqualTo(confirmBefore);
    }

    @Test
    void concurrencyLimiterPermitsUseConfiguredCapOrDefaultToNinetyPercentOfReadPrimaryMaxConnections() {
        final PostgresControlPlane defaultCap = openStandbyProtectedControlPlane(
            primaryDbUrl,
            standbyDbUrl,
            primaryDbUrl,
            Map.of("read.primary.max.connections", "10")
        );
        assertThat(defaultCap.primaryReadLimiter().availablePermits()).isEqualTo(9);

        final PostgresControlPlane singleConnCap = openStandbyProtectedControlPlane(
            primaryDbUrl,
            standbyDbUrl,
            primaryDbUrl,
            Map.of("read.primary.max.connections", "1")
        );
        assertThat(singleConnCap.primaryReadLimiter().availablePermits()).isEqualTo(1);

        final PostgresControlPlane explicitCap = openStandbyProtectedControlPlane(
            primaryDbUrl,
            standbyDbUrl,
            primaryDbUrl,
            Map.of(
                "read.primary.max.connections", "10",
                PostgresControlPlaneConfig.STANDBY_FALLBACK_MAX_CONCURRENT_CONFIG, "3"
            )
        );
        assertThat(explicitCap.primaryReadLimiter().availablePermits()).isEqualTo(3);
    }

    @Test
    void confirmedPartitionAfterExhaustedStandbyBudgetDropsBatchesAndKeepsPrimaryMetadata() {
        final PostgresControlPlane cp = openStandbyProtectedControlPlane(
            primaryDbUrl, standbyDbUrl, primaryDbUrl, Map.of());

        final long confirmBefore = meterCount("FindBatchesPrimaryConfirmRate");
        final long overrideBefore = meterCount("FindBatchesPrimaryOverrideRate");

        // TP1's 256-byte standby batch spends the entire 256-byte fetchMaxBytes budget.
        // TP0 follows in request order and gets confirmed on primary, so it must return no batches
        // while keeping `Errors.NONE`, `logStartOffset`, and primary's `highWatermark` (20).
        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(
                new FindBatchRequest(TP1, 0, 1024 * 1024),
                new FindBatchRequest(TP0, 12, 1024 * 1024)
            ),
            256,
            0
        );

        assertThat(responses).hasSize(2);
        assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(0).batches()).hasSize(1);
        assertThat(responses.get(0).highWatermark()).isEqualTo(5L);

        assertThat(responses.get(1).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(1).batches()).isEmpty();
        assertThat(responses.get(1).logStartOffset()).isEqualTo(0L);
        assertThat(responses.get(1).highWatermark()).isEqualTo(20L);

        assertThat(meterCount("FindBatchesPrimaryConfirmRate")).isEqualTo(confirmBefore + 1);
        assertThat(meterCount("FindBatchesPrimaryOverrideRate")).isEqualTo(overrideBefore + 1);
    }

    @Test
    void confirmedPartitionBeforeHealthyStandbyPreservesRequestOrderBudget() {
        final PostgresControlPlane cp = openStandbyProtectedControlPlane(
            primaryDbUrl, standbyDbUrl, primaryDbUrl, Map.of());

        // TP0 comes first in request order and confirms on primary (256-byte batch).
        // That batch spends the 256-byte fetchMaxBytes budget, so later standby partition TP1
        // keeps its metadata with empty batches.
        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(
                new FindBatchRequest(TP0, 12, 1024 * 1024),
                new FindBatchRequest(TP1, 0, 1024 * 1024)
            ),
            256,
            0
        );

        assertThat(responses).hasSize(2);
        assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(0).batches()).hasSize(1);
        assertThat(responses.get(0).batches().get(0).objectKey()).isEqualTo("obj-1");
        assertThat(responses.get(0).highWatermark()).isEqualTo(20L);

        assertThat(responses.get(1).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(1).batches()).isEmpty();
        assertThat(responses.get(1).logStartOffset()).isEqualTo(0L);
        assertThat(responses.get(1).highWatermark()).isEqualTo(5L);
    }

    @Test
    void zeroPartitionLimitAfterHealthyStandbyDropsPrimaryMinimumBatch() {
        final PostgresControlPlane cp = openStandbyProtectedControlPlane(
            primaryDbUrl, standbyDbUrl, primaryDbUrl, Map.of());

        // TP1 returns a 256-byte batch (within the 1 KiB fetchMaxBytes).
        // TP0 follows with `maxPartitionFetchBytes = 0` and triggers confirmation on primary.
        // Re-running the full request on primary keeps one minimum-batch allowance while
        // preserving TP0's confirmed metadata.
        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(
                new FindBatchRequest(TP1, 0, 1024),
                new FindBatchRequest(TP0, 12, 0)
            ),
            1024,
            0
        );

        assertThat(responses).hasSize(2);
        assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(0).batches()).hasSize(1);
        assertThat(responses.get(0).highWatermark()).isEqualTo(5L);

        assertThat(responses.get(1).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(1).batches()).isEmpty();
        assertThat(responses.get(1).logStartOffset()).isEqualTo(0L);
        assertThat(responses.get(1).highWatermark()).isEqualTo(20L);
    }

    @Test
    void zeroPartitionLimitAfterConfirmedPrimaryDropsStandbyMinimumBatch() {
        final PostgresControlPlane cp = openStandbyProtectedControlPlane(
            primaryDbUrl, standbyDbUrl, primaryDbUrl, Map.of());

        // TP0 triggers confirmation on primary and returns a 256-byte batch (within 1 KiB).
        // TP1 follows with `maxPartitionFetchBytes = 0`. Re-running the full request on primary
        // leaves TP1 empty with its metadata instead of keeping standby's minimum batch.
        final List<FindBatchResponse> responses = cp.findBatches(
            List.of(
                new FindBatchRequest(TP0, 12, 1024),
                new FindBatchRequest(TP1, 0, 0)
            ),
            1024,
            0
        );

        assertThat(responses).hasSize(2);
        assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(0).batches()).hasSize(1);
        assertThat(responses.get(0).batches().get(0).objectKey()).isEqualTo("obj-1");
        assertThat(responses.get(0).highWatermark()).isEqualTo(20L);

        assertThat(responses.get(1).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(1).batches()).isEmpty();
        assertThat(responses.get(1).logStartOffset()).isEqualTo(0L);
        assertThat(responses.get(1).highWatermark()).isEqualTo(5L);
    }

    @Test
    void unreachableStandbyFallsBackImmediatelyTripsCircuitAndRecoversViaProbe() throws Exception {
        final String standbyDbName = extractDbName(standbyDbUrl);
        try (LocalTcpProxy standbyProxy = new LocalTcpProxy(pgContainer.getHost(), pgContainer.getMappedPort(5432))) {
            standbyProxy.start();
            final String proxiedStandbyUrl = "jdbc:postgresql://127.0.0.1:" + standbyProxy.port() + "/" + standbyDbName;

            final PostgresControlPlane cp = openStandbyProtectedControlPlane(
                primaryDbUrl,
                proxiedStandbyUrl,
                primaryDbUrl,
                Map.of(
                    "read.max.connections", "2",
                    "read.connection.pool.timeout.ms", "500",
                    "read.tcp.connect.timeout.ms", "1000",
                    "read.socket.timeout.ms", "1000",
                    PostgresControlPlaneConfig.STANDBY_CIRCUIT_BREAKER_FAILURE_THRESHOLD_CONFIG, "2",
                    PostgresControlPlaneConfig.STANDBY_CIRCUIT_BREAKER_PROBE_INTERVAL_MS_CONFIG, "1000"
                )
            );

            // Warm standby path while healthy: reads standby HWM (10).
            final List<FindBatchResponse> healthyStandby = cp.findBatches(
                List.of(new FindBatchRequest(TP0, 0, 1024 * 1024)), 1024 * 1024, 0);
            assertThat(healthyStandby.get(0).errors()).isEqualTo(Errors.NONE);
            assertThat(healthyStandby.get(0).highWatermark()).isEqualTo(10L);
            assertThat(cp.standbyCircuitBreaker().isOpen()).isFalse();

            // Stop proxy to simulate a standby outage.
            standbyProxy.stop();

            final long fallbackBefore = meterCount("StandbyFallbackRate");
            final long primaryQueryTimeBefore = histogramCount("FindBatchesPrimaryQueryTime");

            // Failure #1: falls back immediately to primary (HWM 20) even before circuit trips.
            final List<FindBatchResponse> preTrip = cp.findBatches(
                List.of(new FindBatchRequest(TP0, 0, 1024 * 1024)), 1024 * 1024, 0);
            assertThat(preTrip.get(0).errors()).isEqualTo(Errors.NONE);
            assertThat(preTrip.get(0).highWatermark()).isEqualTo(20L);
            assertThat(cp.standbyCircuitBreaker().isOpen()).isFalse();
            assertThat(meterCount("StandbyFallbackRate")).isEqualTo(fallbackBefore + 1);

            // Failure #2 trips the circuit OPEN and still falls back to primary.
            final List<FindBatchResponse> tripping = cp.findBatches(
                List.of(new FindBatchRequest(TP0, 0, 1024 * 1024)), 1024 * 1024, 0);
            assertThat(tripping.get(0).errors()).isEqualTo(Errors.NONE);
            assertThat(tripping.get(0).highWatermark()).isEqualTo(20L);
            assertThat(cp.standbyCircuitBreaker().isOpen()).isTrue();
            assertThat(gaugeIntValue("StandbyCircuitOpen")).isEqualTo(1);
            assertThat(meterCount("StandbyFallbackRate")).isEqualTo(fallbackBefore + 2);

            // While OPEN, findBatches skips standby completely (FindBatchesQueryRate doesn't advance).
            final int standbyQueryRateWhileOpen = gaugeIntValue("FindBatchesQueryRate");
            final List<FindBatchResponse> whileOpen = cp.findBatches(
                List.of(new FindBatchRequest(TP0, 0, 1024 * 1024)), 1024 * 1024, 0);
            assertThat(whileOpen.get(0).errors()).isEqualTo(Errors.NONE);
            assertThat(whileOpen.get(0).highWatermark()).isEqualTo(20L);
            assertThat(gaugeIntValue("FindBatchesQueryRate")).isEqualTo(standbyQueryRateWhileOpen);
            assertThat(meterCount("StandbyFallbackRate")).isEqualTo(fallbackBefore + 3);
            assertThat(histogramCount("FindBatchesPrimaryQueryTime")).isEqualTo(primaryQueryTimeBefore + 3);

            // Bring standby back up on the same port and wait for background SELECT 1 probe to close the breaker.
            standbyProxy.start();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (cp.standbyCircuitBreaker().isOpen() && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(cp.standbyCircuitBreaker().isOpen()).isFalse();
            assertThat(gaugeIntValue("StandbyCircuitOpen")).isEqualTo(0);

            // Subsequent fetch resumes reading from the standby (HWM 10).
            final List<FindBatchResponse> afterRecovery = cp.findBatches(
                List.of(new FindBatchRequest(TP0, 0, 1024 * 1024)), 1024 * 1024, 0);
            assertThat(afterRecovery.get(0).errors()).isEqualTo(Errors.NONE);
            assertThat(afterRecovery.get(0).highWatermark()).isEqualTo(10L);
        }
    }

    @Test
    void openCircuitConcurrencyLimitRejectionAndPrimaryFailureReturnKafkaStorageError() throws Exception {
        final String standbyDbName = extractDbName(standbyDbUrl);
        final String primaryDbName = extractDbName(primaryDbUrl);
        try (LocalTcpProxy standbyProxy = new LocalTcpProxy(pgContainer.getHost(), pgContainer.getMappedPort(5432));
             LocalTcpProxy primaryProxy = new LocalTcpProxy(pgContainer.getHost(), pgContainer.getMappedPort(5432))) {
            standbyProxy.start();
            primaryProxy.start();
            final String proxiedStandbyUrl = "jdbc:postgresql://127.0.0.1:" + standbyProxy.port() + "/" + standbyDbName;
            final String proxiedPrimaryUrl = "jdbc:postgresql://127.0.0.1:" + primaryProxy.port() + "/" + primaryDbName;

            final PostgresControlPlane cp = openStandbyProtectedControlPlane(
                primaryDbUrl,
                proxiedStandbyUrl,
                proxiedPrimaryUrl,
                Map.of(
                    "read.max.connections", "1",
                    "read.connection.pool.timeout.ms", "500",
                    "read.primary.max.connections", "1",
                    "read.primary.connection.pool.timeout.ms", "500",
                    PostgresControlPlaneConfig.STANDBY_FALLBACK_MAX_CONCURRENT_CONFIG, "1",
                    PostgresControlPlaneConfig.STANDBY_CIRCUIT_BREAKER_FAILURE_THRESHOLD_CONFIG, "1",
                    PostgresControlPlaneConfig.STANDBY_CIRCUIT_BREAKER_PROBE_INTERVAL_MS_CONFIG, "60000"
                )
            );

            // Trip the circuit open.
            standbyProxy.stop();
            final List<FindBatchResponse> tripped = cp.findBatches(
                List.of(new FindBatchRequest(TP0, 0, 1024 * 1024)), 1024 * 1024, 0);
            assertThat(tripped.get(0).errors()).isEqualTo(Errors.NONE);
            assertThat(cp.standbyCircuitBreaker().isOpen()).isTrue();

            // Exhaust concurrency limiter while OPEN: returns per-partition `KAFKA_STORAGE_ERROR`.
            final long rejectedBefore = meterCount("PrimaryReadLimitRejectedRate");
            final long fallbackFailureBefore = meterCount("StandbyFallbackFailureRate");
            cp.primaryReadLimiter().acquire();
            try {
                final List<FindBatchResponse> rejected = cp.findBatches(
                    List.of(
                        new FindBatchRequest(TP0, 0, 1024 * 1024),
                        new FindBatchRequest(TP1, 0, 1024 * 1024)
                    ),
                    1024 * 1024,
                    0
                );
                assertThat(rejected).hasSize(2);
                assertThat(rejected.get(0).errors()).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
                assertThat(rejected.get(1).errors()).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
            } finally {
                cp.primaryReadLimiter().release();
            }
            assertThat(meterCount("PrimaryReadLimitRejectedRate")).isEqualTo(rejectedBefore + 1);
            assertThat(meterCount("StandbyFallbackFailureRate")).isEqualTo(fallbackFailureBefore + 1);

            // Primary also fails while OPEN: returns per-partition `KAFKA_STORAGE_ERROR` instead of throwing.
            primaryProxy.stop();
            final List<FindBatchResponse> primaryDown = cp.findBatches(
                List.of(
                    new FindBatchRequest(TP0, 0, 1024 * 1024),
                    new FindBatchRequest(TP1, 0, 1024 * 1024)
                ),
                1024 * 1024,
                0
            );
            assertThat(primaryDown).hasSize(2);
            assertThat(primaryDown.get(0).errors()).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
            assertThat(primaryDown.get(1).errors()).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
            assertThat(meterCount("StandbyFallbackFailureRate")).isEqualTo(fallbackFailureBefore + 2);
        }
    }

    private static String extractDbName(final String jdbcUrl) {
        final int slash = jdbcUrl.lastIndexOf('/');
        final int query = jdbcUrl.indexOf('?', slash);
        return query >= 0 ? jdbcUrl.substring(slash + 1, query) : jdbcUrl.substring(slash + 1);
    }

    private String createDatabase(final String name) {
        pgContainer.createDatabase(name);
        return pgContainer.getUserJdbcUrl();
    }

    private PostgresControlPlane openSingleDbControlPlane(final String jdbcUrl) {
        return openControlPlane(Map.of(
            "connection.string", jdbcUrl,
            "username", pgContainer.getUsername(),
            "password", pgContainer.getPassword()
        ));
    }

    private PostgresControlPlane openStandbyProtectedControlPlane(
        final String jobsUrl,
        final String readUrl,
        final String readPrimaryUrl,
        final Map<String, String> extraConfigs
    ) {
        final Map<String, String> configs = new HashMap<>();
        configs.put("connection.string", jobsUrl);
        configs.put("username", pgContainer.getUsername());
        configs.put("password", pgContainer.getPassword());
        configs.put("read.connection.string", readUrl);
        configs.put("read.username", pgContainer.getUsername());
        configs.put("read.password", pgContainer.getPassword());
        configs.put("read.primary.connection.string", readPrimaryUrl);
        configs.put("read.primary.username", pgContainer.getUsername());
        configs.put("read.primary.password", pgContainer.getPassword());
        configs.putAll(extraConfigs);
        return openControlPlane(configs);
    }

    private PostgresControlPlane openControlPlane(final Map<String, String> configs) {
        final PostgresControlPlane cp = new PostgresControlPlane(time);
        cp.configure(configs);
        openedControlPlanes.add(cp);
        return cp;
    }

    private static void commitBatch(
        final PostgresControlPlane cp,
        final String objectKey,
        final TopicIdPartition tp,
        final long baseOffset,
        final long lastOffset,
        final long maxTimestamp
    ) {
        cp.commitFile(
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            FILE_SIZE,
            List.of(CommitBatchRequest.of(
                0, tp, 0, 256, baseOffset, lastOffset, maxTimestamp, TimestampType.CREATE_TIME
            ))
        );
    }

    private static Metric registeredMetric(final String name) {
        final KafkaMetricsGroup group = new KafkaMetricsGroup(
            PostgresControlPlane.class.getPackageName(), PostgresControlPlane.class.getSimpleName());
        return KafkaYammerMetrics.defaultRegistry().allMetrics().get(group.metricName(name, Map.of()));
    }

    private static long meterCount(final String name) {
        return ((Meter) registeredMetric(name)).count();
    }

    private static long histogramCount(final String name) {
        return ((Histogram) registeredMetric(name)).count();
    }

    private static int gaugeIntValue(final String name) {
        return (int) ((Gauge<?>) registeredMetric(name)).value();
    }

    /**
     * Minimal TCP proxy bound to an ephemeral local port. Tests stop and restart it on the same
     * port to simulate a PostgreSQL standby outage and recovery against Testcontainers.
     */
    private static final class LocalTcpProxy implements Closeable {
        private final String targetHost;
        private final int targetPort;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final List<Socket> activeSockets = new CopyOnWriteArrayList<>();
        private int listenPort;
        private ServerSocket serverSocket;
        private Thread acceptThread;

        private LocalTcpProxy(final String targetHost, final int targetPort) {
            this.targetHost = targetHost;
            this.targetPort = targetPort;
        }

        int port() {
            return listenPort;
        }

        synchronized void start() throws IOException {
            if (running.get()) {
                return;
            }
            serverSocket = new ServerSocket(listenPort, 50, InetAddress.getLoopbackAddress());
            serverSocket.setReuseAddress(true);
            listenPort = serverSocket.getLocalPort();
            running.set(true);
            acceptThread = new Thread(this::acceptLoop, "test-pg-proxy-accept-" + listenPort);
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        synchronized void stop() throws IOException {
            running.set(false);
            if (serverSocket != null) {
                serverSocket.close();
                serverSocket = null;
            }
            for (final Socket socket : activeSockets) {
                closeQuietly(socket);
            }
            activeSockets.clear();
            if (acceptThread != null) {
                try {
                    acceptThread.join(2_000L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                acceptThread = null;
            }
        }

        private void acceptLoop() {
            final ServerSocket listener = serverSocket;
            while (running.get() && listener != null && !listener.isClosed()) {
                try {
                    final Socket client = listener.accept();
                    final Socket upstream = new Socket(targetHost, targetPort);
                    activeSockets.add(client);
                    activeSockets.add(upstream);
                    startPipe(client, upstream);
                    startPipe(upstream, client);
                } catch (final IOException ignored) {
                    if (!running.get()) {
                        return;
                    }
                }
            }
        }

        private void startPipe(final Socket from, final Socket to) {
            final Thread pipe = new Thread(() -> {
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    final byte[] buffer = new byte[8192];
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        out.write(buffer, 0, read);
                        out.flush();
                    }
                } catch (final IOException ignored) {
                    // Expected when either side closes or proxy stops.
                } finally {
                    closeQuietly(from);
                    closeQuietly(to);
                    activeSockets.remove(from);
                    activeSockets.remove(to);
                }
            }, "test-pg-proxy-pipe");
            pipe.setDaemon(true);
            pipe.start();
        }

        private static void closeQuietly(final Socket socket) {
            try {
                socket.close();
            } catch (final IOException ignored) {
                // Ignore close errors during simulated outage.
            }
        }

        @Override
        public void close() throws IOException {
            stop();
        }
    }
}
