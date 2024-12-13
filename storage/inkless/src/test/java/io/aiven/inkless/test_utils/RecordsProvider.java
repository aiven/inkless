/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.inkless.test_utils;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.RandomGenerator;
import net.jqwik.api.Shrinkable;
import net.jqwik.api.ShrinkingDistance;
import net.jqwik.api.providers.ArbitraryProvider;
import net.jqwik.api.providers.TypeUsage;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class RecordsProvider implements ArbitraryProvider {

    @Override
    public boolean canProvideFor(TypeUsage targetType) {
        return targetType.isAssignableFrom(Records.class);
    }

    @Override
    public Set<Arbitrary<?>> provideFor(TypeUsage targetType, SubtypeProvider subtypeProvider) {
        RandomGenerator<Byte> randomMagicByte = Arbitraries.bytes()
                .between(RecordBatch.MAGIC_VALUE_V0, RecordBatch.CURRENT_MAGIC_VALUE)
                .generator(1);
        RandomGenerator<Short> randomNonNegativeShort = Arbitraries.shorts().greaterOrEqual((short) 0).generator(1);
        RandomGenerator<Integer> randomInt = Arbitraries.integers().generator(1);
        RandomGenerator<Integer> randomNonNegativeInt = Arbitraries.integers().greaterOrEqual(0).generator(1);
        RandomGenerator<Long> randomLong = Arbitraries.longs().generator(1);
        RandomGenerator<Long> randomNonNegativeLong = Arbitraries.longs().greaterOrEqual(0).generator(1);
        RandomGenerator<Compression> randomCompressionForV1V0 = Arbitraries.of(
                Compression.none().build(),
                Compression.gzip().build(),
                Compression.snappy().build(),
                Compression.lz4().build()
        ).generator(1);
        RandomGenerator<Compression> randomCompressionForV2 = Arbitraries.of(
                Compression.none().build(),
                Compression.gzip().build(),
                Compression.snappy().build(),
                Compression.lz4().build(),
                Compression.zstd().build()
        ).generator(1);
        RandomGenerator<TimestampType> randomTimestampType = Arbitraries.of(
                TimestampType.LOG_APPEND_TIME,
                TimestampType.CREATE_TIME
        ).generator(1);
        RandomGenerator<SimpleRecord[]> simpleRecords = Arbitraries.defaultFor(SimpleRecord.class).array(SimpleRecord[].class).ofMaxSize(255).generator(1);
        return Set.of(Arbitraries.fromGenerator(random -> {
            byte magic = randomMagicByte.next(random).value();
            return new ShrinkableRecords(
                    magic,
                    randomNonNegativeLong.next(random),
                    (magic > 1 ? randomCompressionForV2 : randomCompressionForV1V0).next(random),
                    randomTimestampType.next(random),
                    magic > 1 ? randomNonNegativeLong.next(random) : Shrinkable.unshrinkable(RecordBatch.NO_PRODUCER_ID),
                    randomNonNegativeShort.next(random),
                    randomNonNegativeInt.next(random),
                    randomInt.next(random),
                    magic > 1 && random.nextBoolean(),
                    simpleRecords.next(random)
            );
        }));
    }

    private record ShrinkableRecords(
            byte magic,
            Shrinkable<Long> initialOffset,
            Shrinkable<Compression> compression,
            Shrinkable<TimestampType> timestampType,
            Shrinkable<Long> producerId,
            Shrinkable<Short> producerEpoch,
            Shrinkable<Integer> baseSequence,
            Shrinkable<Integer> partitionLeaderEpoch,
            boolean isTransactional,
            Shrinkable<SimpleRecord[]> simpleRecords
    ) implements Shrinkable<Records> {

        @Override
        public Records value() {
            return MemoryRecords.withRecords(
                    magic,
                    initialOffset.value(),
                    compression.value(),
                    timestampType.value(),
                    producerId.value(),
                    producerEpoch.value(),
                    baseSequence.value(),
                    partitionLeaderEpoch.value(),
                    isTransactional,
                    simpleRecords.value()
            );
        }

        @Override
        public Stream<Shrinkable<Records>> shrink() {
            return Stream.concat(
                    Stream.concat(
                            Stream.concat(
                                    initialOffset.shrink().map(initialOffset -> new ShrinkableRecords(magic, initialOffset, compression, timestampType, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, simpleRecords)),
                                    compression.shrink().map(compression -> new ShrinkableRecords(magic, initialOffset, compression, timestampType, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, simpleRecords))
                            ),
                            Stream.concat(
                                    timestampType.shrink().map(timestampType -> new ShrinkableRecords(magic, initialOffset, compression, timestampType, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, simpleRecords)),
                                    producerId.shrink().map(producerId -> new ShrinkableRecords(magic, initialOffset, compression, timestampType, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, simpleRecords))
                            )
                    ),
                    Stream.concat(
                            Stream.concat(
                                    producerEpoch.shrink().map(producerEpoch -> new ShrinkableRecords(magic, initialOffset, compression, timestampType, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, simpleRecords)),
                                    baseSequence.shrink().map(baseSequence -> new ShrinkableRecords(magic, initialOffset, compression, timestampType, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, simpleRecords))
                            ),
                            Stream.concat(
                                    partitionLeaderEpoch.shrink().map(partitionLeaderEpoch -> new ShrinkableRecords(magic, initialOffset, compression, timestampType, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, simpleRecords)),
                                    simpleRecords.shrink().map(simpleRecords -> new ShrinkableRecords(magic, initialOffset, compression, timestampType, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, simpleRecords))
                            )
                    )
            );
        }

        @Override
        public ShrinkingDistance distance() {
            return ShrinkingDistance.forCollection(List.of(
                    initialOffset.asGeneric(),
                    compression.asGeneric(),
                    timestampType.asGeneric(),
                    producerId.asGeneric(),
                    producerEpoch.asGeneric(),
                    baseSequence.asGeneric(),
                    partitionLeaderEpoch.asGeneric(),
                    simpleRecords.asGeneric()
            ));
        }
    }
}
