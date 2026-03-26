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
package io.aiven.inkless.unification;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.consume.FetchCompleter;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TSConsolidationWalFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(TSConsolidationWalFetcher.class);
    private static final Comparator<TopicIdPartition> TIP_COMPARATOR = Comparator
        .comparing(TopicIdPartition::topicId)
        .thenComparing(TopicIdPartition::partition);

    private final ObjectFetcher objectFetcher;
    private final ObjectKeyCreator objectKeyCreator;

    public TSConsolidationWalFetcher(ObjectFetcher objectFetcher, ObjectKeyCreator objectKeyCreator) {
        this.objectFetcher = objectFetcher;
        this.objectKeyCreator = objectKeyCreator;
    }

    public Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> fetchWalSegments(List<BatchInfo> batchInfos) throws IOException, StorageBackendException {
        var batchInfosByObject = batchInfos.stream().collect(Collectors.groupingBy(bi -> bi.objectKey()));
        var result = new ArrayList<Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>>>();
        for (var entry : batchInfosByObject.entrySet()) {
            result.add(fetchFromRemoteStorage(entry.getKey(), entry.getValue()));
        }
        Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> mergedMap = result
            .stream()
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (existingTreeMap, newTreeMap) -> {
                    // Merge the second TreeMap into the first one
                    existingTreeMap.putAll(newTreeMap);
                    return existingTreeMap;
                }
            ));
        return mergedMap;
    }

    private Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> fetchFromCache(String objectKey, List<BatchInfo> batchInfos) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> fetchFromRemoteStorage(String objectKey, List<BatchInfo> batchInfos) throws IOException, StorageBackendException {
        var resultMap = fetchData(objectKey, batchInfos);
        return byPartition(resultMap);
    }

    private Map<BatchInfo, MemoryRecords> fetchData(String objectKey, List<BatchInfo> batchInfos) throws IOException, StorageBackendException {
        var resultMap = new HashMap<BatchInfo, MemoryRecords>();
        for (BatchInfo batchInfo : batchInfos) {
            var fileExtent = createFileExtent(objectKeyCreator.from(objectKey), batchInfo.metadata().range());
            var memoryRecords = FetchCompleter.constructRecordsFromFile(batchInfo, Collections.singletonList(fileExtent));
            if (memoryRecords != null) {
                resultMap.put(batchInfo, memoryRecords);
            } else  {
                LOGGER.warn("Incomplete batch found for {}", batchInfo);
            }
        }
        return resultMap;
    }

    // visible for testing
    private FileExtent createFileExtent(ObjectKey object, ByteRange byteRange) throws IOException, StorageBackendException {
        final ByteBuffer byteBuffer = objectFetcher.readToByteBuffer(objectFetcher.fetch(object, byteRange));
        return new FileExtent()
            .setObject(object.value())
            .setRange(new FileExtent.ByteRange()
                .setOffset(byteRange.offset())
                .setLength(byteBuffer.limit()))
            .setData(byteBuffer.array());
    }

    private Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> byPartition(Map<BatchInfo, MemoryRecords> batchMap) {
        return batchMap
            .entrySet()
            .stream()
            .collect(Collectors.groupingBy(
                e -> e.getKey().metadata().topicIdPartition(),
                () -> new TreeMap<>(TIP_COMPARATOR),
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (a, b) -> a,
                    () -> new TreeMap<>(Comparator.comparingLong(bi -> bi.metadata().baseOffset()))
                )
            ));
    }
}
