// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;


import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.FilesRecord;
import org.jooq.generated.tables.records.FilesToDeleteRecord;
import org.jooq.generated.tables.records.LogsRecord;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.FILES;
import static org.jooq.generated.Tables.FILES_TO_DELETE;
import static org.jooq.generated.Tables.LOGS;
import static org.jooq.impl.DSL.asterisk;

public class DBUtils {
    static Set<LogsRecord> getAllLogs(final HikariDataSource hikariDataSource) {
        return getAll(hikariDataSource, LOGS, LogsRecord.class);
    }

    static Set<FilesRecord> getAllFiles(final HikariDataSource hikariDataSource) {
        return getAll(hikariDataSource, FILES, FilesRecord.class);
    }

    static Set<FilesToDeleteRecord> getAllFilesToDelete(final HikariDataSource hikariDataSource) {
        return getAll(hikariDataSource, FILES_TO_DELETE, FilesToDeleteRecord.class);
    }

    static Set<BatchesRecord> getAllBatches(final HikariDataSource hikariDataSource) {
        return getAll(hikariDataSource, BATCHES, BatchesRecord.class);
    }

    private static <T extends Record> Set<T> getAll(final HikariDataSource hikariDataSource,
                                                    final TableImpl<T> table,
                                                    final Class<T> recordClass) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.select(asterisk())
                .from(table)
                .fetchStreamInto(recordClass)
                .collect(Collectors.toSet());
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
