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
        RandomGenerator<String> stringGenerator = Arbitraries.strings().ofMaxLength(255).generator(0);
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
