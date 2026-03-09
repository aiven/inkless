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

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Row2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.GetProducerStateRequest;
import io.aiven.inkless.control_plane.GetProducerStateResponse;
import io.aiven.inkless.control_plane.postgres.converters.UUIDtoUuidConverter;

import static org.jooq.generated.Tables.LOGS;
import static org.jooq.generated.Tables.PRODUCER_STATE;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.row;
import static org.jooq.impl.DSL.values;

public class GetProducerStateJob implements Callable<List<GetProducerStateResponse>> {
    private static final Field<Uuid> REQUEST_TOPIC_ID = field(name("topic_id"), LOGS.TOPIC_ID.getDataType());
    private static final Field<Integer> REQUEST_PARTITION = field(name("partition"), LOGS.PARTITION.getDataType());

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<GetProducerStateRequest> requests;
    private final Consumer<Long> durationCallback;

    public GetProducerStateJob(final Time time,
                               final DSLContext jooqCtx,
                               final List<GetProducerStateRequest> requests,
                               final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<GetProducerStateResponse> call() throws Exception {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<GetProducerStateResponse> runOnce() throws Exception {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final DSLContext context = conf.dsl();

            final UUIDtoUuidConverter uuidConverter = new UUIDtoUuidConverter();
            final var requestRows = requests.stream()
                .map(req -> row(uuidConverter.to(req.topicId()), req.partition()))
                .toArray(Row2[]::new);
            @SuppressWarnings("unchecked")
            final var requestsTable = values(requestRows)
                .as("requests", REQUEST_TOPIC_ID.getName(), REQUEST_PARTITION.getName());

            // First, determine which requested partitions actually exist via LOGS table
            final var logsSelect = context.select(
                    requestsTable.field(REQUEST_TOPIC_ID),
                    requestsTable.field(REQUEST_PARTITION),
                    LOGS.LOG_START_OFFSET
                ).from(requestsTable)
                .leftJoin(LOGS).on(LOGS.TOPIC_ID.eq(requestsTable.field(REQUEST_TOPIC_ID))
                    .and(LOGS.PARTITION.eq(requestsTable.field(REQUEST_PARTITION))));

            // Track which partitions exist, preserving request order
            final Map<RequestKey, Boolean> partitionExists = new LinkedHashMap<>();
            try (final var cursor = logsSelect.fetchSize(1000).fetchLazy()) {
                for (final var record : cursor) {
                    final Uuid topicId = uuidConverter.from(record.get(REQUEST_TOPIC_ID.getName(), UUID.class));
                    final Integer partition = record.get(requestsTable.field(REQUEST_PARTITION));
                    final boolean exists = record.get(LOGS.LOG_START_OFFSET) != null;
                    partitionExists.put(new RequestKey(topicId, partition), exists);
                }
            }

            // Fetch all producer state entries for existing partitions
            final Map<RequestKey, List<GetProducerStateResponse.ProducerStateEntry>> entriesByPartition = new LinkedHashMap<>();
            final var existingKeys = partitionExists.entrySet().stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .toList();

            if (!existingKeys.isEmpty()) {
                final var existingRows = existingKeys.stream()
                    .map(key -> row(uuidConverter.to(key.topicId()), key.partition()))
                    .toArray(Row2[]::new);
                @SuppressWarnings("unchecked")
                final var existingTable = values(existingRows)
                    .as("existing", REQUEST_TOPIC_ID.getName(), REQUEST_PARTITION.getName());

                final var producerSelect = context.select(
                        PRODUCER_STATE.TOPIC_ID,
                        PRODUCER_STATE.PARTITION,
                        PRODUCER_STATE.PRODUCER_ID,
                        PRODUCER_STATE.PRODUCER_EPOCH,
                        PRODUCER_STATE.BASE_SEQUENCE,
                        PRODUCER_STATE.LAST_SEQUENCE,
                        PRODUCER_STATE.ASSIGNED_OFFSET,
                        PRODUCER_STATE.BATCH_MAX_TIMESTAMP
                    ).from(PRODUCER_STATE)
                    .innerJoin(existingTable).on(
                        PRODUCER_STATE.TOPIC_ID.eq(existingTable.field(REQUEST_TOPIC_ID))
                            .and(PRODUCER_STATE.PARTITION.eq(existingTable.field(REQUEST_PARTITION))))
                    .orderBy(PRODUCER_STATE.TOPIC_ID, PRODUCER_STATE.PARTITION,
                        PRODUCER_STATE.PRODUCER_ID, PRODUCER_STATE.ROW_ID);

                try (final var cursor = producerSelect.fetchSize(1000).fetchLazy()) {
                    for (final var record : cursor) {
                        final RequestKey key = new RequestKey(
                            record.get(PRODUCER_STATE.TOPIC_ID),
                            record.get(PRODUCER_STATE.PARTITION)
                        );
                        entriesByPartition
                            .computeIfAbsent(key, k -> new ArrayList<>())
                            .add(new GetProducerStateResponse.ProducerStateEntry(
                                record.get(PRODUCER_STATE.PRODUCER_ID),
                                record.get(PRODUCER_STATE.PRODUCER_EPOCH),
                                record.get(PRODUCER_STATE.BASE_SEQUENCE),
                                record.get(PRODUCER_STATE.LAST_SEQUENCE),
                                record.get(PRODUCER_STATE.ASSIGNED_OFFSET),
                                record.get(PRODUCER_STATE.BATCH_MAX_TIMESTAMP)
                            ));
                    }
                }
            }

            // Build responses in request order
            final List<GetProducerStateResponse> responses = new ArrayList<>();
            for (final GetProducerStateRequest request : requests) {
                final RequestKey key = new RequestKey(request.topicId(), request.partition());
                final Boolean exists = partitionExists.get(key);
                if (exists == null || !exists) {
                    responses.add(GetProducerStateResponse.unknownTopicOrPartition());
                } else {
                    final List<GetProducerStateResponse.ProducerStateEntry> entries =
                        entriesByPartition.getOrDefault(key, List.of());
                    responses.add(GetProducerStateResponse.success(entries));
                }
            }
            return responses;
        });
    }

    private record RequestKey(Uuid topicId, int partition) {
    }
}
