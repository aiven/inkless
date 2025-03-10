/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
        RandomGenerator<String> randomString = Arbitraries.strings().ofMaxLength(10).generator(1);
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
