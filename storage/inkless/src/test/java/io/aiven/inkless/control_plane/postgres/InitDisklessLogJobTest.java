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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.generated.tables.records.ProducerStateRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Set;

import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.InvalidDisklessStartOffsetException;
import io.aiven.inkless.control_plane.ProducerStateSnapshot;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class InitDisklessLogJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID1 = new Uuid(10, 12);

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    private void runInitJob(final Set<InitDisklessLogRequest> requests) {
        new InitDisklessLogJob(Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {}).call();
    }

    @Test
    void initializeNewLog() {
        final List<ProducerStateSnapshot> producerStateEntries = List.of(
            new ProducerStateSnapshot(1001L, (short) 1, 0, 9, 100L, 1000L),
            new ProducerStateSnapshot(1002L, (short) 2, 0, 4, 110L, 2000L)
        );
        runInitJob(Set.of(
            new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 200L, 5, producerStateEntries)
        ));

        // Verify log was created
        final var logs = DBUtils.getAllLogs(pgContainer.getDataSource());
        assertThat(logs).hasSize(1);
        final var log = logs.iterator().next();
        assertThat(log.getTopicId()).isEqualTo(TOPIC_ID1);
        assertThat(log.getPartition()).isEqualTo(0);
        assertThat(log.getTopicName()).isEqualTo(TOPIC_1);
        assertThat(log.getLogStartOffset()).isEqualTo(100L);
        assertThat(log.getHighWatermark()).isEqualTo(200L);
        assertThat(log.getDisklessStartOffset()).isEqualTo(200L);
        assertThat(DBUtils.getLeaderEpochAtInit(pgContainer.getDataSource(), TOPIC_ID1, 0)).isEqualTo(5);

        // Verify producer state was created
        final var producerStates = DBUtils.getAllProducerState(pgContainer.getDataSource());
        assertThat(producerStates).hasSize(2);

        final ProducerStateRecord state1 = producerStates.stream()
            .filter(s -> s.getProducerId() == 1001L)
            .findFirst()
            .orElseThrow();
        assertThat(state1.getProducerEpoch()).isEqualTo((short) 1);
        assertThat(state1.getBaseSequence()).isEqualTo(0);
        assertThat(state1.getLastSequence()).isEqualTo(9);
        assertThat(state1.getAssignedOffset()).isEqualTo(100L);
        assertThat(state1.getBatchMaxTimestamp()).isEqualTo(1000L);

        final ProducerStateRecord state2 = producerStates.stream()
            .filter(s -> s.getProducerId() == 1002L)
            .findFirst()
            .orElseThrow();
        assertThat(state2.getProducerEpoch()).isEqualTo((short) 2);
        assertThat(state2.getBaseSequence()).isEqualTo(0);
        assertThat(state2.getLastSequence()).isEqualTo(4);
        assertThat(state2.getAssignedOffset()).isEqualTo(110L);
        assertThat(state2.getBatchMaxTimestamp()).isEqualTo(2000L);
    }

    @Test
    void idempotentWhenLogAlreadyExistsWithSameB0() {
        // First initialization with B0 = 200
        runInitJob(Set.of(new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 200L, 5, List.of())));

        // Second initialization with SAME B0 should be idempotent (no exception, no change)
        final var secondRequests = Set.of(new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 200L, 5, List.of()));
        runInitJob(secondRequests); // Should not throw

        // Verify the log still has the original values (unchanged)
        final var logs = DBUtils.getAllLogs(pgContainer.getDataSource());
        assertThat(logs).hasSize(1);
        final var log = logs.iterator().next();
        assertThat(log.getLogStartOffset()).isEqualTo(100L);
        assertThat(log.getHighWatermark()).isEqualTo(200L);
        assertThat(DBUtils.getLeaderEpochAtInit(pgContainer.getDataSource(), TOPIC_ID1, 0)).isEqualTo(5);
    }

    @Test
    void throwsExceptionWhenDisklessStartOffsetDiffers() {
        // First initialization with disklessStartOffset = 200
        runInitJob(Set.of(new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 200L, 5, List.of())));

        // Second initialization with DIFFERENT disklessStartOffset should throw InvalidDisklessStartOffsetException
        // This indicates a protocol violation that must be investigated
        final var secondRequests = Set.of(new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 300L, 5, List.of()));
        assertThatThrownBy(() -> runInitJob(secondRequests))
            .isInstanceOf(InvalidDisklessStartOffsetException.class)
            .satisfies(e -> {
                final var ex = (InvalidDisklessStartOffsetException) e;
                assertThat(ex.topicId()).isEqualTo(TOPIC_ID1);
                assertThat(ex.partition()).isEqualTo(0);
            });

        // Verify the log still has the original values (unchanged)
        final var logs = DBUtils.getAllLogs(pgContainer.getDataSource());
        assertThat(logs).hasSize(1);
        final var log = logs.iterator().next();
        assertThat(log.getDisklessStartOffset()).isEqualTo(200L);
    }


    @Test
    void multiplePartitionsCanBeInitialized() {
        runInitJob(Set.of(
            new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 200L, 5, List.of()),
            new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 1, 50L, 100L, 3, List.of())
        ));

        final var logs = DBUtils.getAllLogs(pgContainer.getDataSource());
        assertThat(logs).hasSize(2);

        final var partition0 = logs.stream().filter(l -> l.getPartition() == 0).findFirst().orElseThrow();
        assertThat(partition0.getLogStartOffset()).isEqualTo(100L);
        assertThat(partition0.getHighWatermark()).isEqualTo(200L);

        final var partition1 = logs.stream().filter(l -> l.getPartition() == 1).findFirst().orElseThrow();
        assertThat(partition1.getLogStartOffset()).isEqualTo(50L);
        assertThat(partition1.getHighWatermark()).isEqualTo(100L);
    }
}
