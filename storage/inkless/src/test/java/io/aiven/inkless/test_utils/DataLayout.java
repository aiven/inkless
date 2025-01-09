// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.RandomGenerator;
import net.jqwik.api.Shrinkable;
import net.jqwik.api.ShrinkingDistance;
import net.jqwik.api.arbitraries.LongArbitrary;
import net.jqwik.api.arbitraries.StringArbitrary;
import net.jqwik.api.providers.ArbitraryProvider;
import net.jqwik.api.providers.TypeUsage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import io.aiven.inkless.control_plane.BatchInfo;

public record DataLayout (
        Map<BatchInfo, Records> data
) {

    private static final int COLLECTION_MAX_SIZE = 30;

    public static class DataLayoutArbitraryProvider implements ArbitraryProvider {

        @Override
        public boolean canProvideFor(TypeUsage targetType) {
            return targetType.isOfType(DataLayout.class);
        }

        @Override
        public Set<Arbitrary<?>> provideFor(TypeUsage targetType, SubtypeProvider subtypeProvider) {
            StringArbitrary arbitraryString = Arbitraries.strings();
            LongArbitrary arbitraryLong = Arbitraries.longs();
            LongArbitrary arbitraryNonNegativeLong = Arbitraries.longs().greaterOrEqual(0);
            Arbitrary<byte[]> arbitraryByteArray = Arbitraries.bytes().array(byte[].class).ofMaxSize(COLLECTION_MAX_SIZE);
            Arbitrary<Set<TopicIdPartition>> topicIdPartition = Arbitraries.defaultFor(TopicIdPartition.class).set().ofMaxSize(COLLECTION_MAX_SIZE);
            Arbitrary<TimestampType> arbitraryTimestampType = Arbitraries.defaultFor(TimestampType.class);
            Arbitrary<Records> arbitraryRecords = Arbitraries.defaultFor(Records.class);
            return Set.of(Arbitraries.fromGenerator(new DataLayoutRandomGenerator(
                    arbitraryString.generator(1),
                    arbitraryLong.generator(1),
                    arbitraryNonNegativeLong.generator(1),
                    arbitraryByteArray.generator(1),
                    topicIdPartition.generator(1),
                    arbitraryTimestampType.generator(1),
                    arbitraryRecords.generator(1)
            )));
        }
    }

    private record DataLayoutRandomGenerator(
            RandomGenerator<String> randomString,
            RandomGenerator<Long> randomLong,
            RandomGenerator<Long> randomNonNegativeLong,
            RandomGenerator<byte[]> randomByteArray,
            RandomGenerator<Set<TopicIdPartition>> randomTopicPartitions,
            RandomGenerator<TimestampType> randomTimestampType,
            RandomGenerator<Records> randomRecords
    ) implements RandomGenerator<DataLayout> {

        @Override
        public Shrinkable<DataLayout> next(Random random) {
            Shrinkable<Set<TopicIdPartition>> topicPartitions = randomTopicPartitions.next(random);
            Set<TopicIdPartition> initialTopicPartitions = topicPartitions.value();
            if (initialTopicPartitions.isEmpty()) {
                return new ShrinkableDataLayout(Shrinkable.unshrinkable(Collections.emptyList()));
            }
            RandomGenerator<TopicIdPartition> randomTopicPartition = Arbitraries.of(initialTopicPartitions).generator(1);
            RandomGenerator<List<BatchData>> randomBatchData = Arbitraries.fromGenerator(inner -> new ShrinkableBatchData(
                    randomNonNegativeLong.next(inner),
                    randomNonNegativeLong.next(inner),
                    randomTimestampType.next(inner),
                    randomRecords.next(inner),
                    randomLong.next(inner),
                    randomTopicPartition.next(inner).value()
            )).list().ofMaxSize(COLLECTION_MAX_SIZE).generator(1);
            RandomGenerator<List<FileData>> randomFileData = Arbitraries.fromGenerator(inner -> new ShrinkableFileData(
                    randomString.next(inner),
                    randomBatchData.next(inner)
            )).list().ofMaxSize(COLLECTION_MAX_SIZE).generator(1);
            return new ShrinkableDataLayout(randomFileData.next(random));
        }
    }


    private record ShrinkableDataLayout(
            Shrinkable<List<FileData>> files
    ) implements Shrinkable<DataLayout> {

        @Override
        public DataLayout value() {
            Map<BatchInfo, Records> data = new HashMap<>();
            Map<TopicIdPartition, Long> offsets = new HashMap<>();
            for (FileData file : files.value()) {
                long byteOffset = 0L;
                for (BatchData batch : file.batches()) {
                    long firstOffset = offsets.compute(batch.topicIdPartition(), (k, v) -> (v == null ? 0 : v) + batch.skippedOffsets());
                    byteOffset += batch.skippedBytes();
                    BatchInfo batchInfo = BatchInfo.of(
                            file.objectId(),
                            byteOffset,
                            batch.batchSize(),
                            firstOffset,
                            firstOffset,
                            batch.recordCount() - 1, // calculate last offset
                            batch.appendTime(),
                            batch.maxTimestamp(),
                            batch.timestampType()
                    );
                    data.put(batchInfo, batch.records());
                    offsets.compute(batch.topicIdPartition(), (k, v) -> (v == null ? 0 : v) + batch.recordCount());
                    byteOffset += batch.batchSize();
                }
            }
            return new DataLayout(data);
        }

        @Override
        public Stream<Shrinkable<DataLayout>> shrink() {
            return files.shrink().map(ShrinkableDataLayout::new);
        }

        @Override
        public ShrinkingDistance distance() {
            return files.distance();
        }
    }

    private record FileData(String objectId, List<BatchData> batches) {
    }

    private record ShrinkableFileData(
            Shrinkable<String> objectId,
            Shrinkable<List<BatchData>> batches
    ) implements Shrinkable<FileData> {

        @Override
        public FileData value() {
            return new FileData(objectId.value(), batches.value());
        }

        @Override
        public Stream<Shrinkable<FileData>> shrink() {
            return Stream.concat(
                    batches.shrink().map(batches -> new ShrinkableFileData(objectId, batches)),
                    objectId.shrink().map(objectId -> new ShrinkableFileData(objectId, batches))
            );
        }

        @Override
        public ShrinkingDistance distance() {
            return objectId.distance().append(batches.distance());
        }
    }

    private record BatchData(
            long skippedBytes,
            long skippedOffsets,
            TimestampType timestampType,
            Records records,
            int batchSize,
            int recordCount,
            long maxTimestamp,
            long appendTime,
            TopicIdPartition topicIdPartition
    ) {

    }

    private record ShrinkableBatchData(
            Shrinkable<Long> skippedBytes,
            Shrinkable<Long> skippedOffsets,
            Shrinkable<TimestampType> timestampType,
            Shrinkable<Records> records,
            Shrinkable<Long> appendTime,
            TopicIdPartition topicIdPartition
    ) implements Shrinkable<BatchData> {

        @Override
        public BatchData value() {
            Records records = this.records.value();
            int batchSize = records.sizeInBytes();
            int recordCount = 0;
            long maxTimestamp = Long.MIN_VALUE;
            for (RecordBatch batch : records.batches()) {
                maxTimestamp = Math.max(maxTimestamp, batch.maxTimestamp());
                recordCount += getRecordCountInBatch(batch);
            }
            return new BatchData(
                    skippedBytes.value(),
                    skippedOffsets.value(),
                    timestampType.value(),
                    records,
                    batchSize,
                    recordCount,
                    maxTimestamp,
                    appendTime.value(),
                    topicIdPartition
            );
        }

        @Override
        public Stream<Shrinkable<BatchData>> shrink() {
            return Stream.concat(
                    Stream.concat(
                            records.shrink().map(records -> new ShrinkableBatchData(skippedBytes, skippedOffsets, timestampType, records, appendTime, topicIdPartition)),
                            skippedOffsets.shrink().map(skippedOffsets -> new ShrinkableBatchData(skippedBytes, skippedOffsets, timestampType, records, appendTime, topicIdPartition))
                    ),
                    Stream.concat(
                        Stream.concat(
                                skippedBytes.shrink().map(skippedBytes -> new ShrinkableBatchData(skippedBytes, skippedOffsets, timestampType, records, appendTime, topicIdPartition)),
                                skippedOffsets.shrink().map(skippedOffsets -> new ShrinkableBatchData(skippedBytes, skippedOffsets, timestampType, records, appendTime, topicIdPartition))
                        ),
                        Stream.concat(
                                timestampType.shrink().map(timestampType -> new ShrinkableBatchData(skippedBytes, skippedOffsets, timestampType, records, appendTime, topicIdPartition)),
                                appendTime.shrink().map(appendTime -> new ShrinkableBatchData(skippedBytes, skippedOffsets, timestampType, records, appendTime, topicIdPartition))
                        )
                    )
            );
        }

        @Override
        public ShrinkingDistance distance() {
            return skippedBytes.distance()
                    .append(skippedOffsets.distance())
                    .append(timestampType.distance())
                    .append(records.distance())
                    .append(appendTime.distance());
        }

        private static int getRecordCountInBatch(RecordBatch batch) {
            Integer i = batch.countOrNull();
            if (i != null) {
                return i;
            } else {
                int recordCount = 0;
                try (CloseableIterator<Record> iter = batch.streamingIterator(BufferSupplier.create())) {
                    while (iter.hasNext()) {
                        iter.next();
                        recordCount++;
                    }
                }
                return recordCount;
            }
        }
    }
}
