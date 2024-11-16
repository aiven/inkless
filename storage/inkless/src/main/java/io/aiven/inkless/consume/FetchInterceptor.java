// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import io.aiven.inkless.control_plane.MetadataView;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FetchInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchInterceptor.class);

    private final MetadataView metadataView;

    public FetchInterceptor(final MetadataView metadataView) {
        this.metadataView = metadataView;
    }

    public boolean intercept(final FetchParams params,
                             final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                             final Consumer<Map<TopicIdPartition, FetchPartitionData>> responseCallback) {

        final EntrySeparationResult entrySeparationResult = separateEntries(fetchInfos);
        if (entrySeparationResult.bothTypesPresent()) {
            LOGGER.warn("Producing to Inkless and class topic in same request isn't supported");
            final var response = fetchInfos.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            ignored -> new FetchPartitionData(Errors.INVALID_REQUEST, -1, -1,
                                    null, Optional.empty(), OptionalLong.empty(),
                                    Optional.empty(), OptionalInt.empty(), false)));

            responseCallback.accept(response);
            return true;
        }

        // This request produces only to classic topics, don't intercept.
        if (!entrySeparationResult.entitiesForNonInklessTopics.isEmpty()) {
            return false;
        }

        return true;
    }


    private EntrySeparationResult separateEntries(final Map<TopicIdPartition, FetchRequest.PartitionData> entriesPerPartition) {
        final Map<TopicIdPartition, FetchRequest.PartitionData> entitiesForInklessTopics = new HashMap<>();
        final Map<TopicIdPartition, FetchRequest.PartitionData> entitiesForNonInklessTopics = new HashMap<>();
        for (final var entry : entriesPerPartition.entrySet()) {
            if (metadataView.isInklessTopic(entry.getKey().topic())) {
                entitiesForInklessTopics.put(entry.getKey(), entry.getValue());
            } else {
                entitiesForNonInklessTopics.put(entry.getKey(), entry.getValue());
            }
        }
        return new EntrySeparationResult(entitiesForInklessTopics, entitiesForNonInklessTopics);
    }

    private record EntrySeparationResult(Map<TopicIdPartition, FetchRequest.PartitionData> entitiesForInklessTopics,
                                         Map<TopicIdPartition, FetchRequest.PartitionData> entitiesForNonInklessTopics) {
        boolean bothTypesPresent() {
            return !entitiesForInklessTopics.isEmpty() && !entitiesForNonInklessTopics.isEmpty();
        }
    }
}
