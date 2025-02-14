// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import org.jooq.DSLContext;
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

import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.FILES;
import static org.jooq.generated.Tables.FILES_TO_DELETE;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FindFilesToDeleteJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    
    static final String OBJECT_KEY = "a1";
    static final int BROKER_ID = 11;
    static final Instant COMMITTED_AT = Instant.ofEpochMilli(12345);
    static final Instant MARKED_FOR_DELETION_AT = Instant.ofEpochMilli(123456);

    long fileId;

    @Mock
    Time time;

    @BeforeEach
    void setUp(final TestInfo testInfo) throws SQLException {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        try (final Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);

            fileId = ctx.insertInto(FILES,
                FILES.OBJECT_KEY, FILES.REASON, FILES.STATE, FILES.UPLOADER_BROKER_ID, FILES.COMMITTED_AT, FILES.SIZE, FILES.USED_SIZE
            ).values(
                OBJECT_KEY, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, COMMITTED_AT, 1000L, 900L
            ).returning(FILES.FILE_ID).fetchOne(FILES.FILE_ID);

            ctx.insertInto(FILES_TO_DELETE,
                FILES_TO_DELETE.FILE_ID, FILES_TO_DELETE.MARKED_FOR_DELETION_AT
            ).values(
                fileId, MARKED_FOR_DELETION_AT
            ).execute();

            connection.commit();
        }
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void test() {
        final FindFilesToDeleteJob job = new FindFilesToDeleteJob(time, pgContainer.getJooqCtx());
        assertThat(job.call()).containsExactly(
            new FileToDelete(OBJECT_KEY, MARKED_FOR_DELETION_AT)
        );
    }
}
