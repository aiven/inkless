// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

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

public class HeaderProvider implements ArbitraryProvider {
    @Override
    public boolean canProvideFor(TypeUsage targetType) {
        return targetType.isAssignableFrom(Header.class);
    }

    @Override
    public Set<Arbitrary<?>> provideFor(TypeUsage targetType, SubtypeProvider subtypeProvider) {
        RandomGenerator<String> randomString = Arbitraries.strings().generator(1);
        RandomGenerator<byte[]> randomByteArray = Arbitraries.bytes().array(byte[].class).ofMaxSize(10).generator(1);
        return Set.of(Arbitraries.fromGenerator(random -> new ShrinkableHeader(
                randomString.next(random),
                randomByteArray.next(random)
        )));
    }

    private record ShrinkableHeader(
            Shrinkable<String> keyString,
            Shrinkable<byte[]> valueBytes
    ) implements Shrinkable<Header> {
        @Override
        public Header value() {
            return new RecordHeader(keyString.value(), valueBytes.value());
        }

        @Override
        public Stream<Shrinkable<Header>> shrink() {
            return Stream.concat(
                    keyString.shrink().map(keyString -> new ShrinkableHeader(keyString, valueBytes)),
                    valueBytes.shrink().map(valueBytes -> new ShrinkableHeader(keyString, valueBytes))
            );
        }

        @Override
        public ShrinkingDistance distance() {
            return ShrinkingDistance.forCollection(List.of(keyString.asGeneric(), valueBytes.asGeneric()));
        }
    }
}
