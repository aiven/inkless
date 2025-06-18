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

import org.apache.kafka.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

class FunctionResultProcessor {
    /**
     * Map the result of a Postgres function to a list of responses.
     *
     * <p>A Postgres function may reorder requests in safe locking order. This function restores the order defined by the order of requests,
     * matching by topic-partition.
     * Technically there may be multiple requests for one partition, this function handles this situation.
     * @param requests The original requests.
     * @param functionResult The result of the Postgres function.
     * @param requestToKey The mapper from a request to a {@code TopicIdPartition} key.
     * @param recordToKey The mapper from a Postgres record to a {@code TopicIdPartition} key.
     * @param responseMapper The function that maps from a Postgres record and a request to the final response.
     */
    static <Request, FunctionResultRecord, Response> List<Response> processWithMappingOrder(
        final List<Request> requests,
        final List<FunctionResultRecord> functionResult,
        final Function<Request, TopicIdPartition> requestToKey,
        final Function<FunctionResultRecord, TopicIdPartition> recordToKey,
        final BiFunction<Request, FunctionResultRecord, Response> responseMapper
    ) {
        if (functionResult.size() != requests.size()) {
            throw new RuntimeException(String.format("Expected %d items, returned %d", requests.size(), functionResult.size()));
        }

        // Account for potential multiple requests for one partition.
        final Map<TopicIdPartition, List<FunctionResultRecord>> resultMap = new HashMap<>();
        for (var record : functionResult) {
            resultMap
                .computeIfAbsent(recordToKey.apply(record), k -> new ArrayList<>())
                .add(record);
        }

        final List<Response> responses = new ArrayList<>();
        for (final var request : requests) {
            final TopicIdPartition key = requestToKey.apply(request);
            final var records = resultMap.get(key);
            if (records == null || records.isEmpty()) {
                throw new RuntimeException("No result found for request: " + request);
            }
            final var record = records.remove(0);
            responses.add(responseMapper.apply(request, record));
        }
        return responses;
    }
}
