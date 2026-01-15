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

import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.FilesRecord;
import org.jooq.generated.tables.records.LogsRecord;
import org.jooq.generated.tables.records.ProducerStateRecord;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.FILES;
import static org.jooq.generated.Tables.LOGS;
import static org.jooq.generated.Tables.PRODUCER_STATE;
import static org.jooq.impl.DSL.asterisk;

public class DBUtils {
    static Set<LogsRecord> getAllLogs(final HikariDataSource hikariDataSource) {
        return getAll(hikariDataSource, LOGS, LogsRecord.class);
    }

    static Set<FilesRecord> getAllFiles(final HikariDataSource hikariDataSource) {
        return getAll(hikariDataSource, FILES, FilesRecord.class);
    }

    static Set<BatchesRecord> getAllBatches(final HikariDataSource hikariDataSource) {
        return getAll(hikariDataSource, BATCHES, BatchesRecord.class);
    }

    static List<ProducerStateRecord> getAllProducerState(final HikariDataSource hikariDataSource) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.select(asterisk())
                .from(PRODUCER_STATE)
                .orderBy(PRODUCER_STATE.TOPIC_ID, PRODUCER_STATE.PARTITION, PRODUCER_STATE.PRODUCER_ID, PRODUCER_STATE.ROW_ID)
                .fetchInto(ProducerStateRecord.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static Integer getLeaderEpochAtInit(final HikariDataSource hikariDataSource, final Uuid topicId, final int partition) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.select(LOGS.LEADER_EPOCH_AT_INIT)
                .from(LOGS)
                .where(LOGS.TOPIC_ID.eq(topicId)
                    .and(LOGS.PARTITION.eq(partition)))
                .fetchOne(LOGS.LEADER_EPOCH_AT_INIT);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Simulates messages being appended by updating the high_watermark.
     * This makes diskless_start_offset < high_watermark, indicating the log is no longer in migration phase.
     */
    static void simulateMessagesAppended(final HikariDataSource hikariDataSource, final Uuid topicId, final int partition, final long newHighWatermark) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            final int rowsUpdated = ctx.update(LOGS)
                .set(LOGS.HIGH_WATERMARK, newHighWatermark)
                .where(LOGS.TOPIC_ID.eq(topicId)
                    .and(LOGS.PARTITION.eq(partition)))
                .execute();
            if (rowsUpdated == 0) {
                throw new RuntimeException("No rows updated - log entry not found for topic " + topicId + " partition " + partition);
            }
            connection.commit();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
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
