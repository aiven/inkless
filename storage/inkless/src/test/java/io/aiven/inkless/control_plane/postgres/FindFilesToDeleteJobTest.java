// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileState;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FindFilesToDeleteJobTest extends SharedPostgreSQLTest {
    static final String OBJECT_KEY = "a1";
    static final int BROKER_ID = 11;
    static final Instant COMMITTED_AT = Instant.ofEpochMilli(12345);
    static final Instant MARKED_FOR_DELETION_AT = Instant.ofEpochMilli(123456);

    long fileId;

    @Mock
    Time time;

    @BeforeEach
    void insertFile() throws SQLException {
        try (final Connection connection = hikariDataSource.getConnection()) {
            final String insertFileSql = """
                INSERT INTO files(object_key, reason, state, uploader_broker_id, committed_at, size, used_size)
                VALUES(?, ?::file_reason_t, ?::file_state_t, ?, ?, ?, ?)
                RETURNING file_id
                """;
            try (final PreparedStatement preparedStatement = connection.prepareStatement(insertFileSql)) {
                preparedStatement.setString(1, OBJECT_KEY);
                preparedStatement.setString(2, FileReason.PRODUCE.name);
                preparedStatement.setString(3, FileState.UPLOADED.name);
                preparedStatement.setInt(4, BROKER_ID);
                preparedStatement.setTimestamp(5, java.sql.Timestamp.from(COMMITTED_AT));
                preparedStatement.setLong(6, 1000);
                preparedStatement.setLong(7, 900);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    resultSet.next();
                    fileId = resultSet.getLong("file_id");
                };
            }

            final String insertFileToDeleteSql = """
                INSERT INTO files_to_delete(file_id, marked_for_deletion_at)
                VALUES (?, ?)
                """;
            try (final PreparedStatement preparedStatement = connection.prepareStatement(insertFileToDeleteSql)) {
                preparedStatement.setLong(1, fileId);
                preparedStatement.setTimestamp(2, java.sql.Timestamp.from(MARKED_FOR_DELETION_AT));
                preparedStatement.execute();
            }

            connection.commit();
        }
    }

    @Test
    void test() {
        final FindFilesToDeleteJob job = new FindFilesToDeleteJob(time, hikariDataSource);
        assertThat(job.call()).containsExactly(
            new FileToDelete(OBJECT_KEY, MARKED_FOR_DELETION_AT)
        );
    }
}
