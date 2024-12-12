// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.metadata;

import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class InklessTopicMetadataTransformerTest {
    static final String TOPIC_INKLESS = "inkless-topic";
    static final Uuid TOPIC_INKLESS_ID = new Uuid(123, 123);
    static final String TOPIC_CLASSIC = "classic-topic";
    static final Uuid TOPIC_CLASSIC_ID = new Uuid(456, 456);

    @Mock
    MetadataView metadataView;

    @Test
    void nulls() {
        assertThatThrownBy(() -> new InklessTopicMetadataTransformer(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("metadataView cannot be null");

        final var transformer = new InklessTopicMetadataTransformer(metadataView);
        assertThatThrownBy(() -> transformer.transform("x", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicMetadata cannot be null");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"inkless_az=az1", "x=y", ""})
    void emptyMetadata(final String clientId) {
        when(metadataView.getAliveBrokers()).thenReturn(List.of(
            new BrokerMetadata(0, Optional.of("az0")),
            new BrokerMetadata(1, Optional.of("az1"))
        ));

        final var transformer = new InklessTopicMetadataTransformer(metadataView);
        final List<MetadataResponseData.MetadataResponseTopic> topicMetadata = List.of();
        transformer.transform(clientId, topicMetadata);
        assertThat(topicMetadata).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
        "az0,0,2",
        "az1,1,3",
        "az_unknown,0,1",
        ",0,1",
    })
    void inklessAndClassicTopics(final String clientAZ, final int expectedLeaderId1, final int expectedLeaderId2) {
        when(metadataView.isInklessTopic(eq(TOPIC_INKLESS))).thenReturn(true);
        when(metadataView.isInklessTopic(eq(TOPIC_CLASSIC))).thenReturn(false);
        when(metadataView.getAliveBrokers()).thenReturn(List.of(
            new BrokerMetadata(0, Optional.of("az0")),
            new BrokerMetadata(2, Optional.of("az0")),
            new BrokerMetadata(1, Optional.of("az1")),
            new BrokerMetadata(3, Optional.of("az1"))
        ));

        final Supplier<MetadataResponseData.MetadataResponseTopic> inklessTopicMetadata =
            () -> new MetadataResponseData.MetadataResponseTopic()
                .setName(TOPIC_INKLESS)
                .setErrorCode((short) 0)
                .setTopicId(TOPIC_INKLESS_ID)
                .setPartitions(List.of(
                    new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0)
                        .setLeaderId(-1)
                        .setReplicaNodes(List.of(1, 2, 3, 4))
                        .setIsrNodes(List.of(1, 2))
                        .setOfflineReplicas(List.of(3, 4)),
                    new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(1)
                        .setErrorCode((short) 1)
                        .setLeaderId(-1)
                        .setReplicaNodes(List.of(1, 2, 3, 4))
                        .setIsrNodes(List.of(1, 2))
                        .setOfflineReplicas(List.of(3, 4)),
                    new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(2)
                        .setErrorCode((short) 2)
                        .setLeaderId(-1)
                        .setReplicaNodes(List.of(1, 2, 3, 4))
                        .setIsrNodes(List.of(1, 2))
                        .setOfflineReplicas(List.of(3, 4))
                ));

        final Supplier<MetadataResponseData.MetadataResponseTopic> classicTopicMetadata =
            () -> new MetadataResponseData.MetadataResponseTopic()
                .setName(TOPIC_CLASSIC)
                .setErrorCode((short) 0)
                .setTopicId(TOPIC_CLASSIC_ID)
                .setPartitions(List.of(
                    new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0)
                        .setLeaderId(-10)
                        .setReplicaNodes(List.of(1, 2, 3, 4))
                        .setIsrNodes(List.of(1, 2))
                        .setOfflineReplicas(List.of(3, 4))
                ));

        final List<MetadataResponseData.MetadataResponseTopic> topicMetadata = List.of(
            inklessTopicMetadata.get(),
            classicTopicMetadata.get()
        );
        final var transformer = new InklessTopicMetadataTransformer(metadataView);

        transformer.transform("inkless_az=" + clientAZ, topicMetadata);

        final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
        for (final int partition : List.of(0, 1, 2)) {
            setExpectedLeader(expectedInklessTopicMetadata.partitions().get(partition), expectedLeaderId1);
        }

        assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
        assertThat(topicMetadata.get(1)).isEqualTo(classicTopicMetadata.get());

        // Check that rotation happens by transforming again.
        transformer.transform("inkless_az=" + clientAZ, topicMetadata);

        for (final int partition : List.of(0, 1, 2)) {
            setExpectedLeader(expectedInklessTopicMetadata.partitions().get(partition), expectedLeaderId2);
        }

        assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
        assertThat(topicMetadata.get(1)).isEqualTo(classicTopicMetadata.get());
    }

    @Test
    void selectFromAllBrokersWhenBrokerRackIsNotSet() {
        when(metadataView.isInklessTopic(eq(TOPIC_INKLESS))).thenReturn(true);
        when(metadataView.getAliveBrokers()).thenReturn(List.of(
            new BrokerMetadata(1, Optional.empty()),
            new BrokerMetadata(0, Optional.empty())
        ));

        final Supplier<MetadataResponseData.MetadataResponseTopic> inklessTopicMetadata =
            () -> new MetadataResponseData.MetadataResponseTopic()
                .setName(TOPIC_INKLESS)
                .setErrorCode((short) 0)
                .setTopicId(TOPIC_INKLESS_ID)
                .setPartitions(List.of(
                    new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0)
                        .setLeaderId(-1)
                        .setReplicaNodes(List.of(1, 2, 3, 4))
                        .setIsrNodes(List.of(1, 2))
                        .setOfflineReplicas(List.of(3, 4))
                ));

        final List<MetadataResponseData.MetadataResponseTopic> topicMetadata = List.of(inklessTopicMetadata.get());
        final var transformer = new InklessTopicMetadataTransformer(metadataView);

        transformer.transform("inkless_az=az0", topicMetadata);
        final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
        setExpectedLeader(expectedInklessTopicMetadata.partitions().get(0), 0);
        assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);

        transformer.transform("inkless_az=az0", topicMetadata);
        setExpectedLeader(expectedInklessTopicMetadata.partitions().get(0), 1);
        assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
    }

    @Test
    void selectFromAllBrokersWhenClientAZIsNotSet() {
        when(metadataView.isInklessTopic(eq(TOPIC_INKLESS))).thenReturn(true);
        when(metadataView.getAliveBrokers()).thenReturn(List.of(
            new BrokerMetadata(1, Optional.of("az1")),
            new BrokerMetadata(0, Optional.of("az0"))
        ));

        final Supplier<MetadataResponseData.MetadataResponseTopic> inklessTopicMetadata =
            () -> new MetadataResponseData.MetadataResponseTopic()
                .setName(TOPIC_INKLESS)
                .setErrorCode((short) 0)
                .setTopicId(TOPIC_INKLESS_ID)
                .setPartitions(List.of(
                    new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0)
                        .setLeaderId(-1)
                        .setReplicaNodes(List.of(1, 2, 3, 4))
                        .setIsrNodes(List.of(1, 2))
                        .setOfflineReplicas(List.of(3, 4))
                ));

        final List<MetadataResponseData.MetadataResponseTopic> topicMetadata = List.of(inklessTopicMetadata.get());
        final var transformer = new InklessTopicMetadataTransformer(metadataView);

        transformer.transform(null, topicMetadata);
        final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
        setExpectedLeader(expectedInklessTopicMetadata.partitions().get(0), 0);
        assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);

        transformer.transform(null, topicMetadata);
        setExpectedLeader(expectedInklessTopicMetadata.partitions().get(0), 1);
        assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
    }

    private static void setExpectedLeader(final MetadataResponseData. MetadataResponsePartition partition, final int leaderId) {
        partition.setLeaderId(leaderId);
        partition.setReplicaNodes(List.of(leaderId));
        partition.setIsrNodes(List.of(leaderId));
        partition.setOfflineReplicas(Collections.emptyList());
    }
}
