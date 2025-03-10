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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.udt.CommitBatchResponseV1;
import org.jooq.generated.udt.DeleteRecordsResponseV1;
import org.jooq.generated.udt.records.DeleteRecordsRequestV1Record;
import org.jooq.generated.udt.records.DeleteRecordsResponseV1Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;

import static org.jooq.generated.Tables.DELETE_RECORDS_V1;

public class DeleteRecordsJob implements Callable<List<DeleteRecordsResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteRecordsJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<DeleteRecordsRequest> requests;

    public DeleteRecordsJob(final Time time, final DSLContext jooqCtx, final List<DeleteRecordsRequest> requests) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
    }

    @Override
    public List<DeleteRecordsResponse> call() {
        if (requests.isEmpty()) {
            return List.of();
        }

        try {
            return runOnce();
        } catch (final Exception e) {
            // TODO retry with backoff (or not, let the consumers do this?)
            throw new RuntimeException(e);
        }
    }

    private List<DeleteRecordsResponse> runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);
            final DeleteRecordsRequestV1Record[] jooqRequests = requests.stream().map(r ->
                    new DeleteRecordsRequestV1Record(
                        r.topicIdPartition().topicId(),
                        r.topicIdPartition().partition(),
                        r.offset()))
                .toArray(DeleteRecordsRequestV1Record[]::new);

            final List<DeleteRecordsResponseV1Record> functionResult = conf.dsl().select(
                    DeleteRecordsResponseV1.TOPIC_ID,
                    DeleteRecordsResponseV1.PARTITION,
                    DeleteRecordsResponseV1.ERROR,
                    DeleteRecordsResponseV1.LOG_START_OFFSET
                ).from(DELETE_RECORDS_V1.call(now, jooqRequests))
                .fetchInto(DeleteRecordsResponseV1Record.class);
            return processFunctionResult(functionResult);
        });
    }

    private List<DeleteRecordsResponse> processFunctionResult(List<DeleteRecordsResponseV1Record> functionResult) throws SQLException {
        final List<DeleteRecordsResponse> responses = new ArrayList<>();

        final Iterator<DeleteRecordsRequest> iterator = requests.iterator();
        for (final var record : functionResult) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("More records returned than expected");
            }
            final DeleteRecordsRequest request = iterator.next();

            final Uuid requestTopicId = request.topicIdPartition().topicId();
            final int requestPartition = request.topicIdPartition().partition();
            final Uuid resultTopicId = record.getTopicId();
            final int resultPartition = record.get(CommitBatchResponseV1.PARTITION);
            if (!resultTopicId.equals(requestTopicId) || resultPartition != requestPartition) {
                throw new RuntimeException(String.format(
                    "Returned topic ID or partition doesn't match: expected %s-%d, got %s-%d",
                    requestTopicId, requestPartition,
                    resultTopicId, resultPartition
                ));
            }

            if (record.getError() == null) {
                responses.add(DeleteRecordsResponse.success(record.getLogStartOffset()));
            } else {
                final var response = switch (record.getError()) {
                    case unknown_topic_or_partition ->
                        DeleteRecordsResponse.unknownTopicOrPartition();
                    case offset_out_of_range ->
                        DeleteRecordsResponse.offsetOutOfRange();
                    default ->
                        throw new RuntimeException(String.format("Unknown error '%s' returned for %s-%d",
                            record.getError(), resultTopicId, resultPartition));
                };
                responses.add(response);
            }
        }

        if (iterator.hasNext()) {
            throw new RuntimeException("Fewer records returned than expected");
        }

        return responses;
    }
}
