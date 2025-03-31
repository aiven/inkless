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

import io.aiven.inkless.common.ObjectFormat;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class SafeDeleteFileCheckJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    static final int BROKER_ID = 11;

    static final String TOPIC_0 = "topic0";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);

    Time time = new MockTime();

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
    void isSafeToDelete() throws Exception {
        final SafeDeleteFileCheckJob job = new SafeDeleteFileCheckJob(pgContainer.getJooqCtx(), "test");
        assertThat(job.call()).isTrue();
    }

    @Test
    void isNotSafeToDelete() throws Exception {
        final int file1Batch1Size = 1000;
        final int file1Batch2Size = 2000;
        final int file1Size = file1Batch1Size + file1Batch2Size;
        final String objectKey = "test";
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, file1Size,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, file1Batch1Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P1, 0, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ),
            duration -> {}
        ).call();

        final SafeDeleteFileCheckJob job = new SafeDeleteFileCheckJob(pgContainer.getJooqCtx(), objectKey);
        assertThat(job.call()).isFalse();
    }
}
