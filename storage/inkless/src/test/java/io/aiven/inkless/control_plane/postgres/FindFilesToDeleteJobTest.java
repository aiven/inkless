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

import org.apache.kafka.common.utils.Time;

import org.jooq.SQLDialect;
import org.jooq.generated.enums.FileStateT;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.FILES;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FindFilesToDeleteJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    
    static final int BROKER_ID = 11;
    static final Instant COMMITTED_AT = Instant.ofEpochMilli(12345);
    static final Instant MARKED_FOR_DELETION_AT = Instant.ofEpochMilli(123456);

    @Mock
    Time time;

    @BeforeEach
    void setUp(final TestInfo testInfo) throws SQLException {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void returnsEligibleFile() {
        final FileToDelete eligible = insertDeletingFile("a1", MARKED_FOR_DELETION_AT);

        final FindFilesToDeleteJob job = new FindFilesToDeleteJob(
            time, pgContainer.getJooqCtx(), MARKED_FOR_DELETION_AT.plusMillis(1), 0, duration -> {});
        assertThat(job.call()).containsExactly(eligible);
    }

    @Test
    void excludesFileStillInGracePeriod() {
        insertDeletingFile("a1", MARKED_FOR_DELETION_AT);

        // markedBefore is exclusive: a file marked at exactly the boundary is not yet eligible.
        final FindFilesToDeleteJob job = new FindFilesToDeleteJob(
            time, pgContainer.getJooqCtx(), MARKED_FOR_DELETION_AT, 0, duration -> {});
        assertThat(job.call()).isEmpty();
    }

    @Test
    void honoursLimit() {
        insertDeletingFile("a1", MARKED_FOR_DELETION_AT);
        insertDeletingFile("a2", MARKED_FOR_DELETION_AT);
        insertDeletingFile("a3", MARKED_FOR_DELETION_AT);
        final Instant markedBefore = MARKED_FOR_DELETION_AT.plusMillis(1);

        final FindFilesToDeleteJob bounded = new FindFilesToDeleteJob(
            time, pgContainer.getJooqCtx(), markedBefore, 2, duration -> {});
        assertThat(bounded.call()).hasSize(2);

        final FindFilesToDeleteJob unbounded = new FindFilesToDeleteJob(
            time, pgContainer.getJooqCtx(), markedBefore, 0, duration -> {});
        assertThat(unbounded.call()).hasSize(3);
    }

    @Test
    void returnsOnlyEligibleFilesWhenMixed() {
        final Instant markedBefore = MARKED_FOR_DELETION_AT.plusMillis(1);
        final FileToDelete eligible1 = insertDeletingFile("a1", MARKED_FOR_DELETION_AT);
        final FileToDelete eligible2 = insertDeletingFile("a2", MARKED_FOR_DELETION_AT);
        insertDeletingFile("b1", markedBefore);
        insertDeletingFile("b2", markedBefore.plusMillis(1));

        final FindFilesToDeleteJob unbounded = new FindFilesToDeleteJob(
            time, pgContainer.getJooqCtx(), markedBefore, 0, duration -> {});
        assertThat(unbounded.call()).containsExactlyInAnyOrder(eligible1, eligible2);

        // The limit applies after the grace-period predicate: ineligible rows must not consume slots,
        // otherwise a backlog of freshly marked files would starve the eligible ones.
        final FindFilesToDeleteJob bounded = new FindFilesToDeleteJob(
            time, pgContainer.getJooqCtx(), markedBefore, 2, duration -> {});
        assertThat(bounded.call()).hasSize(2);
    }

    private FileToDelete insertDeletingFile(final String objectKey, final Instant markedForDeletionAt) {
        // The container's DataSource has autoCommit=false; commit so the job's connection sees the row.
        try (final Connection connection = pgContainer.getDataSource().getConnection()) {
            DSL.using(connection, SQLDialect.POSTGRES).insertInto(FILES,
                FILES.OBJECT_KEY, FILES.FORMAT, FILES.REASON, FILES.STATE, FILES.UPLOADER_BROKER_ID,
                FILES.COMMITTED_AT, FILES.MARKED_FOR_DELETION_AT, FILES.SIZE
            ).values(
                objectKey, (short) ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT.id, FileReason.PRODUCE, FileStateT.deleting,
                BROKER_ID, COMMITTED_AT, markedForDeletionAt, 1000L
            ).execute();
            connection.commit();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
        return new FileToDelete(objectKey, markedForDeletionAt);
    }
}
