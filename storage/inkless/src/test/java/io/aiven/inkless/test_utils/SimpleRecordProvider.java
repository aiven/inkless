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
