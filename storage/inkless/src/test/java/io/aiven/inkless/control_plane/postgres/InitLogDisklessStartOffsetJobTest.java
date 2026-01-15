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

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.tables.records.LogsRecord;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import io.aiven.inkless.control_plane.InitLogDisklessStartOffsetRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.LOGS;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class InitLogDisklessStartOffsetJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID1 = new Uuid(10, 12);
    static final Uuid TOPIC_ID2 = new Uuid(555, 333);

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void empty() {
        final InitLogDisklessStartOffsetJob job = new InitLogDisklessStartOffsetJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), Set.of(), durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void createLogsWithDisklessStartOffset() {
        final Set<InitLogDisklessStartOffsetRequest> requests = Set.of(
            // logStartOffset, disklessStartOffset (highWatermark)
            new InitLogDisklessStartOffsetRequest(TOPIC_ID1, TOPIC_1, 0, 50L, 100L),
            new InitLogDisklessStartOffsetRequest(TOPIC_ID1, TOPIC_1, 1, 150L, 200L),
            new InitLogDisklessStartOffsetRequest(TOPIC_ID2, TOPIC_2, 0, 25L, 50L)
        );
        final InitLogDisklessStartOffsetJob job = new InitLogDisklessStartOffsetJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {});
        job.run();

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            // LogsRecord: topicId, partition, topicName, logStartOffset, highWatermark, byteSize, disklessStartOffset, disklessEndOffset
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 50L, 100L, 0L, 100L, null),
            new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 150L, 200L, 0L, 200L, null),
            new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 25L, 50L, 0L, 50L, null)
        );
    }

    @Test
    void doesNotOverwriteExistingLog() throws SQLException {
        // Create log that already exists
        try (final Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            ctx.insertInto(LOGS,
                LOGS.TOPIC_ID, LOGS.PARTITION, LOGS.TOPIC_NAME, LOGS.LOG_START_OFFSET, LOGS.HIGH_WATERMARK, LOGS.BYTE_SIZE, LOGS.DISKLESS_START_OFFSET
            ).values(
                TOPIC_ID1, 0, TOPIC_1, 0L, 100L, 999L, 50L
            ).execute();
            connection.commit();
        }

        // Try to create logs - existing one should not be overwritten
        final Set<InitLogDisklessStartOffsetRequest> requests = Set.of(
            new InitLogDisklessStartOffsetRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 100L),  // Should not overwrite existing
            new InitLogDisklessStartOffsetRequest(TOPIC_ID1, TOPIC_1, 1, 200L, 200L)   // Should be created
        );
        final InitLogDisklessStartOffsetJob job = new InitLogDisklessStartOffsetJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {});
        job.run();

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 0L, 100L, 999L, 50L, null),  // Unchanged
            new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 200L, 200L, 0L, 200L, null)  // Created
        );
    }

    @Test
    void idempotentExecution() {
        final Set<InitLogDisklessStartOffsetRequest> requests = Set.of(
            new InitLogDisklessStartOffsetRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 100L)
        );

        // First execution
        final InitLogDisklessStartOffsetJob job1 = new InitLogDisklessStartOffsetJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {});
        job1.run();

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 100L, 100L, 0L, 100L, null)
        );

        // Second execution with same value - should not change anything
        final InitLogDisklessStartOffsetJob job2 = new InitLogDisklessStartOffsetJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {});
        job2.run();

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 100L, 100L, 0L, 100L, null)
        );

        // Third execution with different value - should not overwrite
        final Set<InitLogDisklessStartOffsetRequest> differentRequests = Set.of(
            new InitLogDisklessStartOffsetRequest(TOPIC_ID1, TOPIC_1, 0, 999L, 999L)
        );
        final InitLogDisklessStartOffsetJob job3 = new InitLogDisklessStartOffsetJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), differentRequests, durationMs -> {});
        job3.run();

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 100L, 100L, 0L, 100L, null)  // Still 100L, not 999L
        );
    }
}
