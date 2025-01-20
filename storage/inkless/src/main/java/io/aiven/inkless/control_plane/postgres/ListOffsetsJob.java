// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
            return ListOffsetsResponse.success(request.topicIdPartition(), timestamp, logInfo.logStartOffset());
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
