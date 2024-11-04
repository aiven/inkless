package io.aiven.inkless.reader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import io.aiven.inkless.common.BatchInfo;

class FetchPlanner {
    List<FileFetch> plan(final Map<TopicPartition, List<BatchInfo>> batches) {
        final Map<String, List<BatchInfo>> batchesByFile = groupBatchesByFiles(batches);

        // Sort batches in files by their start offset.
        for (final List<BatchInfo> bathes : batchesByFile.values()) {
            bathes.sort(Comparator.comparing(b -> b.byteOffset));
        }

        final Map<String, List<RangeOfBatches>> rangesByFile = groupBatchesInFilesByRanges(batchesByFile);

        final List<FileFetch> fileFetches = new ArrayList<>();
        for (final Map.Entry<String, List<RangeOfBatches>> entry : rangesByFile.entrySet()) {
            for (final RangeOfBatches range : entry.getValue()) {
                fileFetches.add(
                    new FileFetch(entry.getKey(), range.startOffset(), range.size(), range.batches)
                );
            }
        }
        return fileFetches;
    }

    private Map<String, List<BatchInfo>> groupBatchesByFiles(final Map<TopicPartition, List<BatchInfo>> batches) {
        final Map<String, List<BatchInfo>> result = new HashMap<>();
        for (final List<BatchInfo> batchList : batches.values()) {
            for (final BatchInfo batch : batchList) {
                result.computeIfAbsent(batch.filePath, ignore -> new ArrayList<>())
                    .add(batch);
            }
        }
        return result;
    }

    private Map<String, List<RangeOfBatches>> groupBatchesInFilesByRanges(final Map<String, List<BatchInfo>> batchesByFile) {
        final Map<String, List<RangeOfBatches>> result = new HashMap<>();
        for (final Map.Entry<String, List<BatchInfo>> entry : batchesByFile.entrySet()) {
            final String filePath = entry.getKey();
            result.put(filePath, new ArrayList<>());

            for (final BatchInfo batch : entry.getValue()) {
                final List<RangeOfBatches> batchList = result.get(filePath);
                if (batchList.isEmpty()) {
                    batchList.add(new RangeOfBatches(batch));
                } else {
                    final RangeOfBatches lastRange = batchList.get(batchList.size() - 1);
                    if (lastRange.lastBatch().byteOffset + lastRange.lastBatch().byteSize == batch.byteOffset) {
                        lastRange.addBatch(batch);
                    } else {
                        batchList.add(new RangeOfBatches(batch));
                    }
                }
            }
        }
        return result;
    }

    private static class RangeOfBatches {
        private final List<BatchInfo> batches;

        private RangeOfBatches(final BatchInfo firstBatch) {
            this.batches = new ArrayList<>();
            this.batches.add(firstBatch);
        }

        long startOffset() {
            return batches.get(0).byteOffset;
        }

        long size() {
            return lastBatch().byteOffset + lastBatch().byteSize - startOffset();
        }

        BatchInfo lastBatch() {
            return batches.get(batches.size() - 1);
        }

        void addBatch(final BatchInfo batch) {
            batches.add(batch);
        }
    }
}
