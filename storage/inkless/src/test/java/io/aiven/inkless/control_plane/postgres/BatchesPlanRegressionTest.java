/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that migrations applied on top of the baseline schema do not change the plan of statements that
 * already read or delete {@code batches}. A new index on {@code batches} must pass this test.
 *
 * <p>The fixture mimics the data that made a new index change the plan of retention. Many partitions hold low
 * offsets, a few hold offsets near 10^10, so the table-wide {@code last_offset} histogram makes a range on a
 * high-offset partition look unselective. Rows of different partitions are interleaved in the heap. The test
 * runs once with the covering index at its default fill factor and once rebuilt at a low fill factor, which
 * emulates the index bloat that retention churn causes. A bloated covering index is larger than any new
 * index that shares its {@code (topic_id, partition)} prefix, which is when the planner prefers the new one.
 *
 * <p>Plans come from {@code EXPLAIN} with literal values. PL/pgSQL can plan with other values, so a passing
 * result does not prove the functions are unaffected.
 */
@Testcontainers
class BatchesPlanRegressionTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final String BASELINE_SCHEMA_VERSION = "29";
    static final Uuid TOPIC_ID = new Uuid(5, 6);
    static final String TOPIC_UUID = new UUID(TOPIC_ID.getMostSignificantBits(), TOPIC_ID.getLeastSignificantBits()).toString();
    static final int LOW_PARTITIONS = 1_000;
    static final int HIGH_PARTITIONS = 3;
    static final int LOW_BATCHES = 1_000;
    static final int HIGH_BATCHES = 200_000;
    static final long HIGH_OFFSET_START = 10_000_000_000L;
    static final int LOW_PARTITION = 5;
    static final int HIGH_PARTITION = LOW_PARTITIONS;
    static final int DELETE_ROWS = 500;
    static final int HIGH_DELETE_ROWS = 2_000;

    static final Pattern NODE = Pattern.compile(
        "((?:Parallel )?(?:Seq Scan|Index Only Scan|Index Scan|Bitmap Index Scan|Bitmap Heap Scan))(?: Backward)?(?: using| on) (\\w+)");
    static final Pattern SORT = Pattern.compile("^\\s*(?:->\\s*)?(Incremental Sort|Sort|Gather Merge|Gather)\\b");

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @ParameterizedTest(name = "covering index fillfactor {0}")
    @ValueSource(ints = {90, 10})
    void migrationsDoNotChangeExistingStatementPlans(final int coveringIndexFillFactor) {
        final var before = loadBaseline(coveringIndexFillFactor);
        final Map<String, Long> cuts = before.cuts();

        pgContainer.migrate();
        analyze();

        assertThat(planShapes(cuts)).containsExactlyInAnyOrderEntriesOf(before.shapes());
    }

    /**
     * Guards the preceding check against passing vacuously. An index on the batch timestamp that shares the key
     * prefix of the covering index must make that check fail.
     */
    @Test
    void anIndexSharingTheKeyPrefixChangesRetentionPlans() {
        final var before = loadBaseline(90);

        pgContainer.getJooqCtx().transaction(conf -> DSL.using(conf).execute(
            "CREATE INDEX batches_by_timestamp_idx ON batches "
                + "(topic_id, partition, batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp)) "
                + "INCLUDE (base_offset)"));
        analyze();

        assertThat(planShapes(before.cuts())).isNotEqualTo(before.shapes());
    }

    private record Baseline(Map<String, Long> cuts, Map<String, List<String>> shapes) {
    }

    private Baseline loadBaseline(final int coveringIndexFillFactor) {
        pgContainer.migrate(BASELINE_SCHEMA_VERSION);
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(TOPIC_ID, "plan-fixture", LOW_PARTITIONS + HIGH_PARTITIONS)),
            duration -> { }).run();
        load();
        pgContainer.getJooqCtx().transaction(conf -> {
            if (coveringIndexFillFactor != 90) {
                DSL.using(conf).execute("ALTER INDEX batches_by_last_offset_covering_idx SET (fillfactor = "
                    + coveringIndexFillFactor + ")");
                DSL.using(conf).execute("REINDEX INDEX batches_by_last_offset_covering_idx");
            }
        });
        analyze();
        final Map<String, Long> cuts = Map.of(
            "low", cutOffset(LOW_PARTITION, DELETE_ROWS),
            "high", cutOffset(HIGH_PARTITION, HIGH_DELETE_ROWS));
        return new Baseline(cuts, planShapes(cuts));
    }

    private void analyze() {
        pgContainer.getJooqCtx().transaction(conf -> {
            // A target of 10000 samples 3M rows, so ANALYZE reads the whole fixture and the statistics are
            // the same on every run.
            for (final String column : List.of("topic_id", "partition", "last_offset")) {
                DSL.using(conf).execute("ALTER TABLE batches ALTER COLUMN " + column + " SET STATISTICS 10000");
            }
            DSL.using(conf).execute("ANALYZE batches");
        });
    }

    private Map<String, List<String>> planShapes(final Map<String, Long> cuts) {
        final DSLContext ctx = pgContainer.getJooqCtx();
        final Map<String, List<String>> shapes = new LinkedHashMap<>();
        for (final var partition : Map.of("low", LOW_PARTITION, "high", HIGH_PARTITION).entrySet()) {
            final int p = partition.getValue();
            final long cut = cuts.get(partition.getKey());
            final String key = "WHERE topic_id = '" + TOPIC_UUID + "' AND partition = " + p;
            final Map<String, String> statements = new LinkedHashMap<>();
            statements.put("delete_records_v1 DELETE", "DELETE FROM batches " + key + " AND last_offset < " + cut);
            statements.put("prune DELETE", "DELETE FROM batches " + key + " AND last_offset <= " + cut);
            statements.put("retention recount",
                "SELECT COUNT(*), SUM(byte_size) FROM batches " + key + " AND last_offset < " + cut);
            statements.put("enforce_retention_v2 cap check",
                "SELECT base_offset FROM batches " + key + " AND last_offset < " + cut
                    + " ORDER BY topic_id, partition, last_offset LIMIT 1 OFFSET 400");
            statements.put("delete_records_v1 boundary scan",
                "SELECT last_offset FROM batches " + key + " ORDER BY topic_id, partition, last_offset LIMIT 1 OFFSET 400");
            statements.put("oldest batch", "SELECT base_offset FROM batches " + key + " ORDER BY last_offset LIMIT 1");
            statements.put("find batches",
                "SELECT base_offset, byte_size FROM batches " + key + " AND last_offset >= " + cut
                    + " ORDER BY last_offset LIMIT 100");
            statements.forEach((label, sql) -> shapes.put(partition.getKey() + "-offset partition: " + label, shape(ctx, sql)));
        }
        return shapes;
    }

    private static List<String> shape(final DSLContext ctx, final String sql) {
        final List<String> nodes = new ArrayList<>();
        ctx.fetch("EXPLAIN " + sql).forEach(row -> {
            final String line = row.get(0, String.class);
            final Matcher scan = NODE.matcher(line);
            final Matcher other = SORT.matcher(line);
            if (scan.find()) {
                nodes.add(scan.group(1) + " " + scan.group(2));
            } else if (other.find()) {
                nodes.add(other.group(1));
            }
        });
        return nodes;
    }

    private long cutOffset(final int partition, final int rows) {
        return pgContainer.getJooqCtx().fetchOne(
            "SELECT base_offset FROM batches WHERE topic_id = ?::uuid AND partition = ? ORDER BY last_offset OFFSET ? LIMIT 1",
            TOPIC_UUID, partition, rows).get(0, Long.class);
    }

    private void load() {
        final int partitions = LOW_PARTITIONS + HIGH_PARTITIONS;
        pgContainer.getJooqCtx().transaction(conf -> {
            final var tx = DSL.using(conf);
            final long fileId = tx.fetchOne(
                "INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size) "
                    + "VALUES ('plan-fixture', 1, 'produce', 'uploaded', 1, now(), 0) RETURNING file_id")
                .get(0, Long.class);
            tx.execute("INSERT INTO batches (magic, topic_id, partition, base_offset, last_offset, file_id, byte_offset, "
                + "byte_size, timestamp_type, log_append_timestamp, batch_max_timestamp) "
                + "SELECT 2, ?::uuid, p, CASE WHEN p >= ? THEN ?::bigint ELSE 0 END + i * 10, "
                + "CASE WHEN p >= ? THEN ?::bigint ELSE 0 END + i * 10 + 9, ?, 0, 120, 0, 0, 1700000000000 + i "
                + "FROM generate_series(0, ? - 1) i CROSS JOIN generate_series(0, ? - 1) p ORDER BY i, p",
                TOPIC_UUID, LOW_PARTITIONS, HIGH_OFFSET_START, LOW_PARTITIONS, HIGH_OFFSET_START, fileId,
                LOW_BATCHES, partitions);
            tx.execute("INSERT INTO batches (magic, topic_id, partition, base_offset, last_offset, file_id, byte_offset, "
                + "byte_size, timestamp_type, log_append_timestamp, batch_max_timestamp) "
                + "SELECT 2, ?::uuid, p, ?::bigint + i * 10, ?::bigint + i * 10 + 9, ?, 0, 120, 0, 0, 1700000000000 + i "
                + "FROM generate_series(?, ? - 1) i CROSS JOIN generate_series(?, ? - 1) p ORDER BY i, p",
                TOPIC_UUID, HIGH_OFFSET_START, HIGH_OFFSET_START, fileId,
                LOW_BATCHES, HIGH_BATCHES, LOW_PARTITIONS, partitions);
        });
    }
}
