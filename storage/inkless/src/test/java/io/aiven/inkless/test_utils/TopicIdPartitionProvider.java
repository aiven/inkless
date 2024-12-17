// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
