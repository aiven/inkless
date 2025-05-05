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
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.udt.ListOffsetsResponseV1;
import org.jooq.generated.udt.records.ListOffsetsRequestV1Record;
import org.jooq.generated.udt.records.ListOffsetsResponseV1Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;

import static org.jooq.generated.Tables.LIST_OFFSETS_V1;

public class ListOffsetsJob implements Callable<List<ListOffsetsResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListOffsetsJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<ListOffsetsRequest> requests;

    public ListOffsetsJob(final Time time, final DSLContext jooqCtx, final List<ListOffsetsRequest> requests) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
    }

    @Override
    public List<ListOffsetsResponse> call() {
        try {
            return runOnce();
        } catch (final Exception e) {
            // TODO add retry with backoff (or not, let the consumers do this?)
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private List<ListOffsetsResponse> runOnce() throws Exception {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            try {
                final ListOffsetsRequestV1Record[] jooqRequests = requests.stream().map(r ->
                    new ListOffsetsRequestV1Record(
                        r.topicIdPartition().topicId(),
                        r.topicIdPartition().partition(),
                        r.timestamp()
                    )
                ).toArray(ListOffsetsRequestV1Record[]::new);

                final List<ListOffsetsResponseV1Record> functionResult = conf.dsl().select(
                    ListOffsetsResponseV1.TOPIC_ID,
                    ListOffsetsResponseV1.PARTITION,
                    ListOffsetsResponseV1.TIMESTAMP,
                    ListOffsetsResponseV1.OFFSET,
                    ListOffsetsResponseV1.ERROR
                ).from(LIST_OFFSETS_V1.call(
                    jooqRequests
                )).fetchInto(ListOffsetsResponseV1Record.class);
                return processFunctionResult(functionResult);
            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error listing offsets", e);
            }
        });
    }

    private List<ListOffsetsResponse> processFunctionResult(final List<ListOffsetsResponseV1Record> functionResult) {
        final List<ListOffsetsResponse> responses = new ArrayList<>();
        final Iterator<ListOffsetsRequest> iterator = requests.iterator();
        for (final var record : functionResult) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("More records returned than expected");
            }
            final ListOffsetsRequest request = iterator.next();

            // Sanity check to match returned and requested partitions (they should go in order). Maybe excessive?
            final Uuid requestTopicId = request.topicIdPartition().topicId();
            final int requestPartition = request.topicIdPartition().partition();
            final Uuid resultTopicId = record.getTopicId();
            final int resultPartition = record.get(ListOffsetsResponseV1.PARTITION);
            if (!resultTopicId.equals(requestTopicId) || resultPartition != requestPartition) {
                throw new RuntimeException(String.format(
                    "Returned topic ID or resultPartition doesn't match: expected %s-%d, got %s-%d",
                    requestTopicId, requestPartition,
                    resultTopicId, resultPartition
                ));
            }

            final var response = switch (record.getError()) {
                case none:
                    yield ListOffsetsResponse.success(request.topicIdPartition(), record.getTimestamp(), record.getOffset());
                case unknown_topic_or_partition:
                    yield ListOffsetsResponse.unknownTopicOrPartition(request.topicIdPartition());
                case unsupported_special_timestamp:
                    yield ListOffsetsResponse.unknownServerError(request.topicIdPartition());
            };

            responses.add(response);
        }

        if (iterator.hasNext()) {
            throw new RuntimeException("Fewer records returned than expected");
        }

        return responses;
    }
}
