/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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
import org.apache.kafka.common.record.SimpleRecord;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.RandomGenerator;
import net.jqwik.api.Shrinkable;
import net.jqwik.api.ShrinkingDistance;
import net.jqwik.api.providers.ArbitraryProvider;
import net.jqwik.api.providers.TypeUsage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class SimpleRecordProvider implements ArbitraryProvider {

    @Target({ ElementType.ANNOTATION_TYPE, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface HeaderCount {
        int max();
    }

    @Override
    public boolean canProvideFor(TypeUsage targetType) {
        return targetType.isAssignableFrom(SimpleRecord.class);
    }

    @Override
    public Set<Arbitrary<?>> provideFor(TypeUsage targetType, SubtypeProvider subtypeProvider) {
        RandomGenerator<Long> randomLong = Arbitraries.longs().greaterOrEqual(0).generator(1);
        RandomGenerator<byte[]> randomByteArray = Arbitraries.bytes().array(byte[].class).ofMaxSize(10).generator(1);
        int maxHeaders = targetType.findAnnotation(HeaderCount.class).map(HeaderCount::max).orElse(10);
        RandomGenerator<Header[]> randomHeaders = Arbitraries.defaultFor(Header.class)
                .array(Header[].class)
                .ofMaxSize(maxHeaders)
                .generator(1);
        return Set.of(Arbitraries.fromGenerator(random -> new ShrinkableSimpleRecord(
                randomLong.next(random),
                randomByteArray.next(random),
                randomByteArray.next(random),
                randomHeaders.next(random)
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
