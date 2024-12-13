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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

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

public class TopicIdPartitionProvider implements ArbitraryProvider {
    @Override
    public boolean canProvideFor(TypeUsage targetType) {
        return targetType.isOfType(TopicIdPartition.class);
    }

    @Override
    public Set<Arbitrary<?>> provideFor(TypeUsage targetType, SubtypeProvider subtypeProvider) {
        RandomGenerator<Long> longGenerator = Arbitraries.longs().generator(0);
        RandomGenerator<Integer> intGenerator = Arbitraries.integers().generator(0);
        RandomGenerator<String> stringGenerator = Arbitraries.strings().generator(0);
        return Set.of(Arbitraries.fromGenerator(random -> new ShrinkableTopicIdPartition(
                longGenerator.next(random),
                longGenerator.next(random),
                intGenerator.next(random),
                stringGenerator.next(random)
        )));
    }

    private record ShrinkableTopicIdPartition(
            Shrinkable<Long> mostSigBits,
            Shrinkable<Long> leastSigBits,
            Shrinkable<Integer> partition,
            Shrinkable<String> topic
    ) implements Shrinkable<TopicIdPartition> {
        @Override
        public TopicIdPartition value() {
            return new TopicIdPartition(new Uuid(mostSigBits.value(), leastSigBits.value()), partition.value(), topic.value());
        }

        @Override
        public Stream<Shrinkable<TopicIdPartition>> shrink() {
            return Stream.concat(
                    Stream.concat(
                            topic.shrink().map(topic -> new ShrinkableTopicIdPartition(mostSigBits, leastSigBits, partition, topic)),
                            partition.shrink().map(partition -> new ShrinkableTopicIdPartition(mostSigBits, leastSigBits, partition, topic))
                    ),
                    Stream.concat(
                            mostSigBits.shrink().map(mostSigBits -> new ShrinkableTopicIdPartition(mostSigBits, leastSigBits, partition, topic)),
                            leastSigBits.shrink().map(leastSigBits -> new ShrinkableTopicIdPartition(mostSigBits, leastSigBits, partition, topic))
                    )
            );
        }

        @Override
        public ShrinkingDistance distance() {
            return ShrinkingDistance.forCollection(List.of(
                    mostSigBits.asGeneric(),
                    leastSigBits.asGeneric(),
                    partition.asGeneric(),
                    topic.asGeneric()
            ));
        }
    }
}
