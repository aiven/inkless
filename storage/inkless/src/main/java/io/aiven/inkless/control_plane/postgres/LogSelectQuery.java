// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
