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
