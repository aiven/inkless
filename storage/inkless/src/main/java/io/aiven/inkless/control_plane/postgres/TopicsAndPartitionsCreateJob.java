// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;

import static org.jooq.generated.Tables.LOGS;

public class TopicsAndPartitionsCreateJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicsAndPartitionsCreateJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final Set<CreateTopicAndPartitionsRequest> requests;
    private final Consumer<Long> durationCallback;

    TopicsAndPartitionsCreateJob(final Time time,
                                 final DSLContext jooqCtx,
                                 final Set<CreateTopicAndPartitionsRequest> requests,
                                 final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public void run() {
        if (requests.isEmpty()) {
            return;
        }

        try {
            runOnce();
        } catch (final Exception e) {
            // TODO retry with backoff
            throw new RuntimeException(e);
        }
    }

    private void runOnce() {
        // See how topics are created in ReplicationControlManager.createTopic.
        // It's ordered so that ConfigRecords go after TopicRecord but before PartitionRecord(s).
        // So it means we will see topic configs before any partition.

        jooqCtx.transaction((final Configuration conf) -> {
            var insertStep = conf.dsl().insertInto(LOGS,
                LOGS.TOPIC_ID,
                LOGS.PARTITION,
                LOGS.TOPIC_NAME,
                LOGS.LOG_START_OFFSET,
                LOGS.HIGH_WATERMARK);
            for (final var request : requests) {
                for (int partition = 0; partition < request.numPartitions(); partition++) {
                    insertStep = insertStep.values(request.topicId(), partition, request.topicName(), 0L, 0L);
                }
            }
            final int rowsInserted = insertStep.onConflictDoNothing().execute();

            // This is not expected to happen, but checking just in case.
            final int maxInserts = requests.stream().mapToInt(CreateTopicAndPartitionsRequest::numPartitions).sum();
            if (rowsInserted < 0 || rowsInserted > maxInserts) {
                throw new RuntimeException(
                    String.format("Unexpected number of inserted rows: expected max %d, got %d", maxInserts, rowsInserted));
            }
        });
    }
}
