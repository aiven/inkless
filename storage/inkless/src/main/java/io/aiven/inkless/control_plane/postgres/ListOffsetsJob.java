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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;

public class ListOffsetsJob implements Callable<List<ListOffsetsResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListOffsetsJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<ListOffsetsRequest> requests;
    private final Consumer<Long> getLogsDurationCallback;

    public ListOffsetsJob(Time time, DSLContext jooqCtx, List<ListOffsetsRequest> requests, Consumer<Long> getLogsDurationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.getLogsDurationCallback = getLogsDurationCallback;
    }

    @Override
    public List<ListOffsetsResponse> call() {
        try {
            return runOnce();
        } catch (final Exception e) {
            // TODO add retry with backoff (or not, let the consumers do this?)
            throw new RuntimeException(e);
        }
    }

    private List<ListOffsetsResponse> runOnce() throws Exception {
        final Map<TopicIdPartition, LogEntity> logInfos = getLogInfos(jooqCtx.dsl());
        final List<ListOffsetsResponse> result = new ArrayList<>();
        for (final ListOffsetsRequest request : requests) {
            result.add(listOffset(request, logInfos));
        }
        return result;
    }

    private ListOffsetsResponse listOffset(ListOffsetsRequest request, Map<TopicIdPartition, LogEntity> data) {
        LogEntity logInfo = data.get(request.topicIdPartition());

        if (logInfo == null) {
            LOGGER.warn("Unexpected non-existing partition {}", request.topicIdPartition());
            return ListOffsetsResponse.unknownTopicOrPartition(request.topicIdPartition());
        }

        long timestamp = request.timestamp();
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), timestamp, logInfo.logStartOffset());
        } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), timestamp, logInfo.highWatermark());
        }
        LOGGER.error("listOffset request for timestamp {} in {} unsupported", timestamp, request.topicIdPartition());
        return new ListOffsetsResponse(Errors.UNKNOWN_SERVER_ERROR, request.topicIdPartition(), -1, -1);
    }

    private Map<TopicIdPartition, LogEntity> getLogInfos(final DSLContext context) throws Exception {
        if (requests.isEmpty()) {
            return Map.of();
        }

        final List<TopicIdPartition> tidps = requests.stream()
                .map(ListOffsetsRequest::topicIdPartition)
                .toList();
        return LogSelectQuery.execute(time, context, tidps, false, getLogsDurationCallback).stream()
                .collect(Collectors.toMap(LogEntity::topicIdPartition, Function.identity()));
    }
}
