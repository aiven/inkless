/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
import org.apache.kafka.common.utils.Time;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record5;
import org.jooq.SelectConditionStep;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import io.aiven.inkless.TimeUtils;

import static org.jooq.generated.Tables.LOGS;

class LogSelectQuery {
    static List<LogEntity> execute(
        final Time time,
        final DSLContext jooqCtx,
        final Collection<TopicIdPartition> topicIdAndPartitions,
        final boolean forUpdate,
        final Consumer<Long> durationCallback
    ) throws Exception {
        Objects.requireNonNull(jooqCtx, "jooqCtx cannot be null");
        Objects.requireNonNull(topicIdAndPartitions, "topicIdAndPartitions cannot be null");
        if (topicIdAndPartitions.isEmpty()) {
            throw new IllegalArgumentException("topicIdAndPartitions cannot be empty");
        }

        return TimeUtils.measureDurationMs(time, ()  -> getLogEntities(jooqCtx, topicIdAndPartitions, forUpdate), durationCallback);
    }

    private static List<LogEntity> getLogEntities(final DSLContext jooqCtx,
                                                  final Collection<TopicIdPartition> topicIdAndPartitions,
                                                  final boolean forUpdate) throws SQLException {
        if (topicIdAndPartitions.isEmpty()) {
            return List.of();
        }

        var select = jooqCtx.select(
            LOGS.TOPIC_ID,
            LOGS.PARTITION,
            LOGS.TOPIC_NAME,
            LOGS.LOG_START_OFFSET,
            LOGS.HIGH_WATERMARK
        ).from(LOGS);

        SelectConditionStep<Record5<Uuid, Integer, String, Long, Long>> selectConditional = null;
        for (final TopicIdPartition topicIdPartition : topicIdAndPartitions) {
            final Condition topicIdCondition = LOGS.TOPIC_ID.eq(topicIdPartition.topicId());
            final Condition partitionCondition = LOGS.PARTITION.eq(topicIdPartition.partition());

            if (selectConditional == null) {
                selectConditional = select.where(topicIdCondition.and(partitionCondition));
            } else {
                selectConditional = selectConditional.or(topicIdCondition.and(partitionCondition));
            }
        }

        final Stream<Record5<Uuid, Integer, String, Long, Long>> resultStream;
        if (forUpdate) {
            resultStream = selectConditional.forUpdate().fetchStream();
        } else {
            resultStream = selectConditional.fetchStream();
        }

        try (final var stream = resultStream) {
            return stream.map(r ->
                new LogEntity(
                    r.get(LOGS.TOPIC_ID),
                    r.get(LOGS.PARTITION),
                    r.get(LOGS.TOPIC_NAME),
                    r.get(LOGS.LOG_START_OFFSET),
                    r.get(LOGS.HIGH_WATERMARK)
                )
            ).toList();
        }
    }
}
