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
import org.jooq.Field;
import org.jooq.Row2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.postgres.converters.UUIDtoUuidConverter;

import static org.jooq.generated.Tables.LOGS;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.row;
import static org.jooq.impl.DSL.values;

public class GetLogInfoJob implements Callable<List<GetLogInfoResponse>> {
    private static final Field<Uuid> REQUEST_TOPIC_ID = field(name("topic_id"), LOGS.TOPIC_ID.getDataType());
    private static final Field<Integer> REQUEST_PARTITION = field(name("partition"), LOGS.PARTITION.getDataType());

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<GetLogInfoRequest> requests;
    private final Consumer<Long> durationCallback;

    public GetLogInfoJob(final Time time,
                         final DSLContext jooqCtx,
                         final List<GetLogInfoRequest> requests,
                         final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<GetLogInfoResponse> call() throws Exception {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<GetLogInfoResponse> runOnce() throws Exception {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final DSLContext context = conf.dsl();

            final UUIDtoUuidConverter uuidConverter = new UUIDtoUuidConverter();
            final var requestRows = requests.stream()
                .map(req -> row(uuidConverter.to(req.topicId()), req.partition()))
                .toArray(Row2[]::new);
            @SuppressWarnings("unchecked")
            final var requestsTable = values(requestRows)
                .as("requests", REQUEST_TOPIC_ID.getName(), REQUEST_PARTITION.getName());

            final var select = context.select(
                    requestsTable.field(REQUEST_TOPIC_ID),
                    requestsTable.field(REQUEST_PARTITION),
                    LOGS.LOG_START_OFFSET,
                    LOGS.HIGH_WATERMARK,
                    LOGS.BYTE_SIZE
                ).from(requestsTable)
                .leftJoin(LOGS).on(LOGS.TOPIC_ID.eq(requestsTable.field(REQUEST_TOPIC_ID))
                    .and(LOGS.PARTITION.eq(requestsTable.field(REQUEST_PARTITION))));

            final List<GetLogInfoResponse> responses = new ArrayList<>();
            try (final var cursor = select.fetchSize(1000).fetchLazy()) {
                for (final var record : cursor) {
                    final Long logStartOffset = record.get(LOGS.LOG_START_OFFSET);
                    if (logStartOffset == null) {
                        responses.add(GetLogInfoResponse.unknownTopicOrPartition());
                    } else {
                        responses.add(GetLogInfoResponse.success(
                            logStartOffset,
                            record.get(LOGS.HIGH_WATERMARK),
                            record.get(LOGS.BYTE_SIZE)
                        ));
                    }
                }
            }
            return responses;
        });
    }
}
