package io.aiven.inkless.cache;

import org.apache.kafka.common.TopicIdPartition;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

import io.aiven.inkless.control_plane.BatchCoordinate;

public class MemoryBatchCoordinateCache implements BatchCoordinateCache {

    private final ItemLevelEvictionCache<TopicIdPartition, BatchCoordinate> backingCache = new ItemLevelEvictionCache<>(10);

    @Override
    public void close() throws IOException {
        backingCache.clear();
    }

    @Override
    public List<BatchCoordinate> get(TopicIdPartition topicIdPartition, long offset) {
        var allBatches = backingCache.getList(topicIdPartition);
        if (allBatches == null) {
            return null;
        }
        return getSublistFromMatch(allBatches, batchCoordinate -> offset <= batchCoordinate.lastOffset() && offset >= batchCoordinate.baseOffset());
    }

    @Override
    public void put(TopicIdPartition topicIdPartition, BatchCoordinate batchCoordinate) {

    }

    @Override
    public boolean remove(TopicIdPartition topicIdPartition) {
        return false;
    }

//    @Override
//    public long size() {
//        return 0;
//    }

    private List<BatchCoordinate> getSublistFromMatch(
        LinkedList<BatchCoordinate> originalList,
        Predicate<BatchCoordinate> matchCondition
    ) {
        if (originalList == null || originalList.isEmpty()) {
            return new LinkedList<>(); // Return an empty list for null or empty input
        }

        LinkedList<BatchCoordinate> resultList = new LinkedList<>();
        ListIterator<BatchCoordinate> iterator = originalList.listIterator(); // Use ListIterator

        // Phase 1: Iterate to find the first matching element
        while (iterator.hasNext()) {
            BatchCoordinate currentElement = iterator.next();
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
