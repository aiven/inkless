// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;

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

public class SimpleRecordProvider implements ArbitraryProvider {
    @Override
    public boolean canProvideFor(TypeUsage targetType) {
        return targetType.isAssignableFrom(SimpleRecord.class);
    }

    @Override
    public Set<Arbitrary<?>> provideFor(TypeUsage targetType, SubtypeProvider subtypeProvider) {
        RandomGenerator<Long> randomLong = Arbitraries.longs().greaterOrEqual(0).generator(1);
        RandomGenerator<byte[]> randomByteArray = Arbitraries.bytes().array(byte[].class).ofMaxSize(10).generator(1);
        RandomGenerator<Header[]> randomHeaders = Arbitraries.defaultFor(Header.class).array(Header[].class).ofMaxSize(10).generator(1);
        return Set.of(Arbitraries.fromGenerator(random -> new ShrinkableSimpleRecord(
                randomLong.next(random),
                randomByteArray.next(random),
                randomByteArray.next(random),
                Shrinkable.unshrinkable(Record.EMPTY_HEADERS) //TODO: Generate headers randomHeaders.next(random)
        )));
    }

    private record ShrinkableSimpleRecord(
            Shrinkable<Long> timestamp,
            Shrinkable<byte[]> keyBytes,
            Shrinkable<byte[]> valueBytes,
            Shrinkable<Header[]> headers
    ) implements Shrinkable<SimpleRecord> {

        @Override
        public SimpleRecord value() {
            return new SimpleRecord(timestamp.value(), keyBytes.value(), valueBytes.value(), headers.value());
        }

        @Override
        public Stream<Shrinkable<SimpleRecord>> shrink() {
            return Stream.concat(
                    Stream.concat(
                            timestamp.shrink().map(timestamp -> new ShrinkableSimpleRecord(timestamp, keyBytes, valueBytes, headers)),
                            keyBytes.shrink().map(keyBytes -> new ShrinkableSimpleRecord(timestamp, keyBytes, valueBytes, headers))
                    ),
                    Stream.concat(
                            valueBytes.shrink().map(valueBytes -> new ShrinkableSimpleRecord(timestamp, keyBytes, valueBytes, headers)),
                            headers.shrink().map(headers -> new ShrinkableSimpleRecord(timestamp, keyBytes, valueBytes, headers))
                    )
            );
        }

        @Override
        public ShrinkingDistance distance() {
            return ShrinkingDistance.combine(List.of(
                    timestamp.asGeneric(),
                    keyBytes.asGeneric(),
                    valueBytes.asGeneric(),
                    headers.asGeneric()
            ));
        }
    }
}
