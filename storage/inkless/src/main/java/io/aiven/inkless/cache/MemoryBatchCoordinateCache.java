package io.aiven.inkless.cache;

import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;

public class MemoryBatchCoordinateCache implements BatchCoordinateCache {

    private final ItemLevelEvictionCache<TopicIdPartition, BatchInfo> backingCache = new ItemLevelEvictionCache<>(10);

    @Override
    public void close() throws IOException {
        backingCache.clear();
    }

    @Override
    public List<BatchInfo> get(BatchCoordinateCacheKey key) {
        var allBatches = backingCache.getList(key.topicIdPartition());
        if (allBatches == null) {
            return null;
        }
        return getSublistFromMatch(allBatches, batchInfo -> key.offset() <= batchInfo.metadata().lastOffset() && key.offset() >= batchInfo.metadata().baseOffset());
    }

    @Override
    public void put(BatchCoordinateCacheKey key, List<BatchInfo> value) {

    }

    @Override
    public boolean remove(BatchCoordinateCacheKey key) {
        return false;
    }

    @Override
    public long size() {
        return 0;
    }

    private List<BatchInfo> getSublistFromMatch(
        LinkedList<BatchInfo> originalList,
        Predicate<BatchInfo> matchCondition
    ) {
        if (originalList == null || originalList.isEmpty()) {
            return new LinkedList<>(); // Return an empty list for null or empty input
        }

        LinkedList<BatchInfo> resultList = new LinkedList<>();
        ListIterator<BatchInfo> iterator = originalList.listIterator(); // Use ListIterator

        // Phase 1: Iterate to find the first matching element
        while (iterator.hasNext()) {
            BatchInfo currentElement = iterator.next();
            if (matchCondition.test(currentElement)) {
                // "Certain entry" found, add it to the result
                resultList.add(currentElement);

                // Phase 2: Add all remaining subsequent entries
                while (iterator.hasNext()) {
                    resultList.add(iterator.next());
                }
                // Once the match is found and the rest are copied, we can stop.
                break;
            }
        }
        if (resultList.isEmpty()) {
            return null;
        }
        return resultList;
    }
}
