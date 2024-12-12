// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import io.aiven.inkless.control_plane.BatchInfo;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.RandomGenerator;
import net.jqwik.api.Shrinkable;
import net.jqwik.api.ShrinkingDistance;
import net.jqwik.api.providers.ArbitraryProvider;
import net.jqwik.api.providers.TypeUsage;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

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
            return Set.of(Arbitraries.fromGenerator(new DataLayoutRandomGenerator(subtypeProvider)));
        }
    }

    private record DataLayoutRandomGenerator(
            ArbitraryProvider.SubtypeProvider subtypeProvider
    ) implements RandomGenerator<DataLayout> {

        @Override
        public Shrinkable<DataLayout> next(Random random) {
            return new DataLayoutShrinkable(random);
        }
    }

    private record DataLayoutShrinkable(
            Random random
    ) implements Shrinkable<DataLayout> {

        @Override
        public DataLayout value() {
            Map<TopicIdPartition, Long> partitionOffsets = new HashMap<>();
            iterate(() -> generateTopicIdPartition(id -> partitionOffsets.put(id, random.nextLong())));
            List<TopicIdPartition> partitions = new ArrayList<>(partitionOffsets.keySet());

            Map<BatchInfo, Records> data = new HashMap<>();
            if (!partitionOffsets.isEmpty()) {
                iterate(() -> generateFile(partitions, partitionOffsets, data::put));
            }
            return new DataLayout(data);
        }

        private void iterate(Runnable runnable) {
            int count = random.nextInt(10);
            for (int i = 0; i < count; i++) {
                runnable.run();
            }
        }

        private <T> T iterate(Function<T, T> operation, T initial) {
            AtomicReference<T> current = new AtomicReference<>(initial);
            iterate(() -> current.set(operation.apply(current.get())));
            return current.get();
        }

        private void generateTopicIdPartition(Consumer<TopicIdPartition> consumer) {
            consumer.accept(new TopicIdPartition(new Uuid(random.nextLong(), random.nextLong()), random.nextInt(), randomString()));
        }

        private String randomString() {
            return "" + random.nextInt();
        }

        private void generateFile(List<TopicIdPartition> partitions, Map<TopicIdPartition, Long> partitionOffsets, BiConsumer<BatchInfo, Records> put) {
            iterate(byteOffset -> {
                long skippedBytes = random.nextLong();
                long skippedOffsets = random.nextLong();
                byte magic = randomMagicByte();
                TimestampType timestampType = randomTimestampType(magic);
                Records records = randomRecords(magic, timestampType);
                int batchSize = records.sizeInBytes();
                int recordCount = countRecordsInBatch(records);
                TopicIdPartition topicIdPartition = randomPartition(partitions);
                long firstOffset = partitionOffsets.compute(topicIdPartition, (k, v) -> (v == null ? 0 : v) + skippedOffsets);
                partitionOffsets.compute(topicIdPartition, (k, v) -> (v == null ? 0 : v) + recordCount);
                BatchInfo batchInfo = new BatchInfo(randomString(), byteOffset + skippedBytes, batchSize, firstOffset, recordCount, timestampType, random.nextLong());
                put.accept(batchInfo, records);
                return skippedBytes + batchSize;
            }, 0L);
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

        private TopicIdPartition randomPartition(List<TopicIdPartition> partitions) {
            int i = random.nextInt(partitions.size());
            return partitions.get(i);
        }

        private byte randomMagicByte() {
            return (byte) random.nextInt(RecordBatch.CURRENT_MAGIC_VALUE + 1);
        }

        private Records randomRecords(byte magic, TimestampType timestampType) {
            return MemoryRecords.withRecords(
                    magic,
                    0L,
                    randomCompression(magic),
                    timestampType,
                    magic > 1 ? random.nextLong() : RecordBatch.NO_PRODUCER_ID,
                    (short) random.nextInt(Short.MAX_VALUE + 1),
                    Math.max(0, random.nextInt()),
                    random.nextInt(),
                    magic > 1 && random.nextBoolean(),
                    randomSimpleRecords(magic)
            );
        }

        private Compression randomCompression(byte magic) {
            int compressionIndex = random.nextInt(CompressionType.values().length);
            if (magic <= 1) {
                compressionIndex = Math.min(3, compressionIndex);
            }
            return Compression.of(CompressionType.values()[compressionIndex]).build();
        }

        private TimestampType randomTimestampType(byte magic) {
            int timestampIndex = random.nextInt(TimestampType.values().length);
            if (magic >= 0) {
                timestampIndex = Math.max(1, timestampIndex);
            }
            return TimestampType.values()[timestampIndex];
        }

        private SimpleRecord[] randomSimpleRecords(byte magic) {
            List<SimpleRecord> records = new ArrayList<>();
            iterate(() -> generateRecord(magic, records::add));
            return records.toArray(new SimpleRecord[0]);
        }

        private void generateRecord(byte magic, Consumer<SimpleRecord> consumer) {
            consumer.accept(new SimpleRecord(
                    Math.max(0, random.nextLong()),
                    randomByteArray(),
                    randomByteArray(),
                    magic > 1 ? randomHeaders() : Record.EMPTY_HEADERS
            ));
        }

        private byte[] randomByteArray() {
            int count = Math.abs(random.nextInt(100));
            byte[] array = new byte[count];
            random.nextBytes(array);
            return array;
        }

        private Header[] randomHeaders() {
            List<Header> headers = new ArrayList<>();
            iterate(() -> generateHeader(headers::add));
            return headers.toArray(new Header[0]);
        }

        private void generateHeader(Consumer<Header> consumer) {
            consumer.accept(new RecordHeader(randomString(), randomByteArray()));
        }

        @Override
        public Stream<Shrinkable<DataLayout>> shrink() {
            return Stream.empty();
        }

        @Override
        public ShrinkingDistance distance() {
            return ShrinkingDistance.MAX;
        }
    }
}
