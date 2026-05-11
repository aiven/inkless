package io.aiven.inkless.control_plane.postgres;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsResponse;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.udt.PruneBatchesBelowHighestTieredOffsetResponseV1;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetRequestV1Record;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetResponseV1Record;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.jooq.generated.Tables.PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V1;

public class PruneDisklessLogsJob implements Callable<List<PruneDisklessLogsResponse>> {

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<PruneDisklessLogsRequest> requests;
    private final Consumer<Long> durationCallback;

    public PruneDisklessLogsJob(Time time, DSLContext jooqCtx, List<PruneDisklessLogsRequest> requests,
                                Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<PruneDisklessLogsResponse> call() throws Exception {
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<PruneDisklessLogsResponse> runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);
            final PruneBatchesBelowHighestTieredOffsetRequestV1Record[] jooqRequests = requests.stream().map(r ->
                    new PruneBatchesBelowHighestTieredOffsetRequestV1Record(
                        r.topicIdPartition().topicId(),
                        r.topicIdPartition().partition(),
                        r.highestRemoteOffset()))
                .toArray(PruneBatchesBelowHighestTieredOffsetRequestV1Record[]::new);

            final List<PruneBatchesBelowHighestTieredOffsetResponseV1Record> functionResult = conf.dsl().select(
                    PruneBatchesBelowHighestTieredOffsetResponseV1.TOPIC_ID,
                    PruneBatchesBelowHighestTieredOffsetResponseV1.PARTITION,
                    PruneBatchesBelowHighestTieredOffsetResponseV1.LOG_START_OFFSET
                ).from(PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V1.call(now, jooqRequests))
                .fetchInto(PruneBatchesBelowHighestTieredOffsetResponseV1Record.class);
            return FunctionResultProcessor.processWithMappingOrder(
                requests,
                functionResult,
                // We don't care about the topic name for the key.
                r -> new TopicIdPartition(r.topicIdPartition().topicId(), r.topicIdPartition().partition(), null),
                r -> new TopicIdPartition(r.getTopicId(), r.getPartition(), null),
                this::responseMapper
            );
        });
    }

    private PruneDisklessLogsResponse responseMapper(final PruneDisklessLogsRequest request,
                                                     final PruneBatchesBelowHighestTieredOffsetResponseV1Record record) {
        return new PruneDisklessLogsResponse(
            new TopicIdPartition(record.getTopicId(), record.getPartition(), null),
            record.getLogStartOffset());
    }
}
