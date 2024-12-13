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
import net.jqwik.api.arbitraries.IntegerArbitrary;
import net.jqwik.api.arbitraries.LongArbitrary;
import net.jqwik.api.arbitraries.StringArbitrary;
import net.jqwik.api.providers.ArbitraryProvider;
import net.jqwik.api.providers.TypeUsage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.inkless.control_plane.BatchInfo;

public record DataLayout (
        Map<BatchInfo, Records> data
) {

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
            IntegerArbitrary arbitrarySize = Arbitraries.integers().lessOrEqual(30);
            Arbitrary<byte[]> arbitraryByteArray = Arbitraries.bytes().array(byte[].class).ofMaxSize(255);
            Arbitrary<Set<TopicIdPartition>> topicIdPartition = Arbitraries.defaultFor(TypeUsage.of(Set.class, TypeUsage.forType(TopicIdPartition.class)));
            Arbitrary<TimestampType> arbitraryTimestampType = Arbitraries.defaultFor(TimestampType.class);
            Arbitrary<Records> arbitraryRecords = Arbitraries.defaultFor(Records.class);
            return Set.of(Arbitraries.fromGenerator(new DataLayoutRandomGenerator(
                    arbitraryString.generator(1),
                    arbitraryLong.generator(1),
                    arbitraryNonNegativeLong.generator(1),
                    arbitrarySize.generator(1),
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
            RandomGenerator<Integer> randomSize,
            RandomGenerator<byte[]> randomByteArray,
            RandomGenerator<Set<TopicIdPartition>> randomTopicPartitions,
            RandomGenerator<TimestampType> randomTimestampType,
            RandomGenerator<Records> randomRecords
    ) implements RandomGenerator<DataLayout> {

        @Override
        public Shrinkable<DataLayout> next(Random random) {
            Shrinkable<Set<TopicIdPartition>> topicPartitions = randomTopicPartitions.next(random);
            Map<TopicIdPartition, Shrinkable<Long>> initialOffsets = new HashMap<>();
            for (TopicIdPartition topicIdPartition : topicPartitions.value()) {
                initialOffsets.put(topicIdPartition, randomNonNegativeLong.next(random));
            }
            RandomGenerator<TopicIdPartition> chosenTopicPartition = Arbitraries.of(initialOffsets.keySet()).generator(1);
            Shrinkable<Integer> numFiles = initialOffsets.isEmpty() ? Shrinkable.unshrinkable(0) : randomSize.next(random);
            int fileCount = numFiles.value();
            Map<ShrinkableFileData, List<ShrinkableBatchData>> batchData = new HashMap<>();
            for (int i = 0; i < fileCount; i++) {
                Shrinkable<Integer> numBatches = randomSize.next(random);
                int batchCount = numBatches.value();
                List<ShrinkableBatchData> batches = new ArrayList<>();
                for (int j = 0; j < batchCount; j++) {
                    batches.add(new ShrinkableBatchData(
                            randomNonNegativeLong.next(random),
                            randomNonNegativeLong.next(random),
                            randomTimestampType.next(random),
                            randomRecords.next(random),
                            randomLong.next(random),
                            chosenTopicPartition.next(random)
                    ));
                }
                batchData.put(new ShrinkableFileData(
                        randomString.next(random)
                ), batches);
            }
            return new ShrinkableDataLayout(topicPartitions, initialOffsets, batchData);
        }
    }

    private record ShrinkableFileData(
            Shrinkable<String> objectId
    ) {

    }

    private record ShrinkableBatchData(
            Shrinkable<Long> skippedBytes,
            Shrinkable<Long> skippedOffsets,
            Shrinkable<TimestampType> timestampType,
            Shrinkable<Records> records,
            Shrinkable<Long> appendTime,
            Shrinkable<TopicIdPartition> topicIdPartition
    ) {
    }

    private record ShrinkableDataLayout(
            Shrinkable<Set<TopicIdPartition>> topicPartitions,
            Map<TopicIdPartition, Shrinkable<Long>> initialOffsets,
            Map<ShrinkableFileData, List<ShrinkableBatchData>> batchData
    ) implements Shrinkable<DataLayout> {

        @Override
        public DataLayout value() {
            Map<BatchInfo, Records> data = new HashMap<>();

            Map<TopicIdPartition, Long> partitionOffsets = initialOffsets.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().value()));
            for (Map.Entry<ShrinkableFileData, List<ShrinkableBatchData>> files : batchData.entrySet()) {
                long byteOffset = 0L;
                for (ShrinkableBatchData batch : files.getValue()) {
                    long skippedBytes = batch.skippedBytes.value();
                    long skippedOffsets = batch.skippedOffsets.value();
                    TimestampType timestampType = batch.timestampType.value();
                    Records records = batch.records.value();
                    int batchSize = records.sizeInBytes();
                    int recordCount = countRecordsInBatch(records);
                    TopicIdPartition topicIdPartition = batch.topicIdPartition.value();
                    long firstOffset = partitionOffsets.compute(topicIdPartition, (k, v) -> (v == null ? 0 : v) + skippedOffsets);
                    partitionOffsets.compute(topicIdPartition, (k, v) -> (v == null ? 0 : v) + recordCount);
                    BatchInfo batchInfo = new BatchInfo(files.getKey().objectId.value(), byteOffset + skippedBytes, batchSize, firstOffset, recordCount, timestampType, batch.appendTime.value());
                    data.put(batchInfo, records);
                    byteOffset += skippedBytes + batchSize;
                }
            }
            return new DataLayout(data);
        }

        private int countRecordsInBatch(Records records) {
            int recordCount = 0;
            for (RecordBatch batch : records.batches()) {
                Integer i = batch.countOrNull();
                if (i != null) {
                    recordCount += i;
                } else {
                    try (CloseableIterator<Record> iter = batch.streamingIterator(BufferSupplier.create())) {
                        while (iter.hasNext()) {
                            iter.next();
                            recordCount++;
                        }
                    }
                }
            }
            return recordCount;
        }

        @Override
        public Stream<Shrinkable<DataLayout>> shrink() {
            return Stream.concat(
                    removeFiles(),
                    removeTopicPartitions()
            );
        }

        private Stream<Shrinkable<DataLayout>> removeFiles() {
            return this.batchData.keySet().stream().map(fileData -> {
                Map<ShrinkableFileData, List<ShrinkableBatchData>> batchData = this.batchData.entrySet()
                        .stream()
                        .filter(e -> e.getKey() == fileData)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return new ShrinkableDataLayout(topicPartitions, initialOffsets, batchData);
            });
        }

        private Stream<Shrinkable<DataLayout>> removeTopicPartitions() {
            return this.topicPartitions.shrink().map(topicPartitions -> {
                Set<TopicIdPartition> removedTopicPartitions = this.topicPartitions.value();
                removedTopicPartitions.removeAll(topicPartitions.value());
                HashMap<TopicIdPartition, Shrinkable<Long>> initialOffsets = new HashMap<>(this.initialOffsets);
                removedTopicPartitions.forEach(initialOffsets::remove);
                Map<ShrinkableFileData, List<ShrinkableBatchData>> batchData = this.batchData.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> e.getValue().stream().filter(batch -> removedTopicPartitions.contains(batch.topicIdPartition.value())).collect(Collectors.toList())
                        ));
                return new ShrinkableDataLayout(topicPartitions, initialOffsets, batchData);
            });
        }

        @Override
        public ShrinkingDistance distance() {
            List<Shrinkable<?>> inner = new ArrayList<>();
            inner.add(topicPartitions);
            return ShrinkingDistance.forCollection(inner.stream().map(Shrinkable::asGeneric).toList());
        }
    }
}
