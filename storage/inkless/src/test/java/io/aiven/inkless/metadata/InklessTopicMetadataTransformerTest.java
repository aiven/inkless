// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.metadata;

import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopicCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
        assertThatThrownBy(() -> transformer.transformClusterMetadata("x", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicMetadata cannot be null");
        assertThatThrownBy(() -> transformer.transformDescribeTopicResponse("x", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("responseData cannot be null");
    }

    @Nested
    class EmptyMetadata {
        @BeforeEach
        void setup() {
            when(metadataView.getAliveBrokers()).thenReturn(List.of(
                new BrokerMetadata(0, Optional.of("az0")),
                new BrokerMetadata(1, Optional.of("az1"))
            ));
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(strings = {"inkless_az=az1", "x=y", ""})
        void clusterMetadata(final String clientId) {
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            final List<MetadataResponseTopic> topicMetadata = List.of();
            transformer.transformClusterMetadata(clientId, topicMetadata);
            assertThat(topicMetadata).isEmpty();
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(strings = {"inkless_az=az1", "x=y", ""})
        void describeTopicResponse(final String clientId) {
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            final DescribeTopicPartitionsResponseData describeResponse = new DescribeTopicPartitionsResponseData();
            transformer.transformDescribeTopicResponse(clientId, describeResponse);
            assertThat(describeResponse).isEqualTo(new DescribeTopicPartitionsResponseData());
        }
    }

    @Nested
    class InklessAndClassicTopics {
        @BeforeEach
        void setup() {
            when(metadataView.isInklessTopic(eq(TOPIC_INKLESS))).thenReturn(true);
            when(metadataView.isInklessTopic(eq(TOPIC_CLASSIC))).thenReturn(false);
            when(metadataView.getAliveBrokers()).thenReturn(List.of(
                new BrokerMetadata(0, Optional.of("az0")),
                new BrokerMetadata(2, Optional.of("az0")),
                new BrokerMetadata(1, Optional.of("az1")),
                new BrokerMetadata(3, Optional.of("az1"))
            ));
        }

        @ParameterizedTest
        @CsvSource({
            "az0,0,2",
            "az1,1,3",
            "az_unknown,0,1",
            ",0,1",
        })
        void clusterMetadata(final String clientAZ, final int expectedLeaderId1, final int expectedLeaderId2) {
            final Supplier<MetadataResponseTopic> inklessTopicMetadata =
                () -> new MetadataResponseTopic()
                    .setName(TOPIC_INKLESS)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_INKLESS_ID)
                    .setPartitions(List.of(
                        new MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4)),
                        new MetadataResponsePartition()
                            .setPartitionIndex(1)
                            .setErrorCode((short) 1)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4)),
                        new MetadataResponsePartition()
                            .setPartitionIndex(2)
                            .setErrorCode((short) 2)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                    ));

            final Supplier<MetadataResponseTopic> classicTopicMetadata =
                () -> new MetadataResponseTopic()
                    .setName(TOPIC_CLASSIC)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_CLASSIC_ID)
                    .setPartitions(List.of(
                        new MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-10)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                    ));

            final List<MetadataResponseTopic> topicMetadata = List.of(
                inklessTopicMetadata.get(),
                classicTopicMetadata.get()
            );
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            transformer.transformClusterMetadata("inkless_az=" + clientAZ, topicMetadata);

            final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
            for (final int partition : List.of(0, 1, 2)) {
                setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(partition), expectedLeaderId1);
            }

            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
            assertThat(topicMetadata.get(1)).isEqualTo(classicTopicMetadata.get());

            // Check that rotation happens by transforming again.
            transformer.transformClusterMetadata("inkless_az=" + clientAZ, topicMetadata);

            for (final int partition : List.of(0, 1, 2)) {
                setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(partition), expectedLeaderId2);
            }

            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
            assertThat(topicMetadata.get(1)).isEqualTo(classicTopicMetadata.get());
        }

        @ParameterizedTest
        @CsvSource({
            "az0,0,2",
            "az1,1,3",
            "az_unknown,0,1",
            ",0,1",
        })
        void describeTopicResponse(final String clientAZ, final int expectedLeaderId1, final int expectedLeaderId2) {
            final Supplier<DescribeTopicPartitionsResponseTopic> inklessTopicMetadata =
                () -> new DescribeTopicPartitionsResponseTopic()
                    .setName(TOPIC_INKLESS)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_INKLESS_ID)
                    .setPartitions(List.of(
                        new DescribeTopicPartitionsResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setEligibleLeaderReplicas(List.of(10, 11))
                            .setLastKnownElr(List.of(10, 11)),
                        new DescribeTopicPartitionsResponsePartition()
                            .setPartitionIndex(1)
                            .setErrorCode((short) 1)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setEligibleLeaderReplicas(List.of(10, 11))
                            .setLastKnownElr(List.of(10, 11)),
                        new DescribeTopicPartitionsResponsePartition()
                            .setPartitionIndex(2)
                            .setErrorCode((short) 2)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setEligibleLeaderReplicas(List.of(10, 11))
                            .setLastKnownElr(List.of(10, 11))
                    ));

            final Supplier<DescribeTopicPartitionsResponseTopic> classicTopicMetadata =
                () -> new DescribeTopicPartitionsResponseTopic()
                    .setName(TOPIC_CLASSIC)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_CLASSIC_ID)
                    .setPartitions(List.of(
                        new DescribeTopicPartitionsResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-10)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setEligibleLeaderReplicas(List.of(10, 11))
                            .setLastKnownElr(List.of(10, 11))
                    ));

            final DescribeTopicPartitionsResponseData describeResponse =
                new DescribeTopicPartitionsResponseData()
                    .setTopics(new DescribeTopicPartitionsResponseTopicCollection(List.of(
                        inklessTopicMetadata.get(),
                        classicTopicMetadata.get()
                    ).iterator()));
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            transformer.transformDescribeTopicResponse("inkless_az=" + clientAZ, describeResponse);

            final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
            for (final int partition : List.of(0, 1, 2)) {
                setExpectedLeaderDescribeTopicResponse(expectedInklessTopicMetadata.partitions().get(partition), expectedLeaderId1);
            }

            assertThat(describeResponse.topics().find(TOPIC_INKLESS)).isEqualTo(expectedInklessTopicMetadata);
            assertThat(describeResponse.topics().find(TOPIC_CLASSIC)).isEqualTo(classicTopicMetadata.get());

            // Check that rotation happens by transforming again.
            transformer.transformDescribeTopicResponse("inkless_az=" + clientAZ, describeResponse);

            for (final int partition : List.of(0, 1, 2)) {
                setExpectedLeaderDescribeTopicResponse(expectedInklessTopicMetadata.partitions().get(partition), expectedLeaderId2);
            }

            assertThat(describeResponse.topics().find(TOPIC_INKLESS)).isEqualTo(expectedInklessTopicMetadata);
            assertThat(describeResponse.topics().find(TOPIC_CLASSIC)).isEqualTo(classicTopicMetadata.get());
        }
    }

    @Nested
    class SelectFromAllBrokersWhenBrokerRackIsNotSetCluster {
        @BeforeEach
        void setup() {
            when(metadataView.isInklessTopic(eq(TOPIC_INKLESS))).thenReturn(true);
            when(metadataView.getAliveBrokers()).thenReturn(List.of(
                new BrokerMetadata(1, Optional.empty()),
                new BrokerMetadata(0, Optional.empty())
            ));
        }

        @Test
        void clusterMetadata() {
            final Supplier<MetadataResponseTopic> inklessTopicMetadata =
                () -> new MetadataResponseTopic()
                    .setName(TOPIC_INKLESS)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_INKLESS_ID)
                    .setPartitions(List.of(
                        new MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                    ));

            final List<MetadataResponseTopic> topicMetadata = List.of(inklessTopicMetadata.get());
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            transformer.transformClusterMetadata("inkless_az=az0", topicMetadata);
            final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), 0);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);

            transformer.transformClusterMetadata("inkless_az=az0", topicMetadata);
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), 1);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
        }

        @Test
        void describeTopicResponse() {
            final Supplier<DescribeTopicPartitionsResponseData> describeResponseSupplier =
                () -> new DescribeTopicPartitionsResponseData()
                    .setTopics(new DescribeTopicPartitionsResponseTopicCollection(List.of(
                        new DescribeTopicPartitionsResponseTopic()
                            .setName(TOPIC_INKLESS)
                            .setErrorCode((short) 0)
                            .setTopicId(TOPIC_INKLESS_ID)
                            .setPartitions(List.of(
                                new DescribeTopicPartitionsResponsePartition()
                                    .setPartitionIndex(0)
                                    .setErrorCode((short) 0)
                                    .setLeaderId(-1)
                                    .setReplicaNodes(List.of(1, 2, 3, 4))
                                    .setIsrNodes(List.of(1, 2))
                                    .setOfflineReplicas(List.of(3, 4))
                                    .setEligibleLeaderReplicas(List.of(10, 11))
                                    .setLastKnownElr(List.of(10, 11))
                            ))
                    ).iterator()));

            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            final DescribeTopicPartitionsResponseData describeResponse = describeResponseSupplier.get();
            transformer.transformDescribeTopicResponse("inkless_az=az0", describeResponse);

            final var expectedDescribeResponse = describeResponseSupplier.get();
            setExpectedLeaderDescribeTopicResponse(expectedDescribeResponse.topics().find(TOPIC_INKLESS).partitions().get(0), 0);
            assertThat(describeResponse).isEqualTo(expectedDescribeResponse);

            transformer.transformDescribeTopicResponse("inkless_az=az0", describeResponse);
            setExpectedLeaderDescribeTopicResponse(expectedDescribeResponse.topics().find(TOPIC_INKLESS).partitions().get(0), 1);
            assertThat(describeResponse).isEqualTo(expectedDescribeResponse);
        }
    }

    @Nested
    class SelectFromAllBrokersWhenClientAZIsNotSetCluster {
        @BeforeEach
        void setup() {
            when(metadataView.isInklessTopic(eq(TOPIC_INKLESS))).thenReturn(true);
            when(metadataView.getAliveBrokers()).thenReturn(List.of(
                new BrokerMetadata(1, Optional.of("az1")),
                new BrokerMetadata(0, Optional.of("az0"))
            ));
        }

        @Test
        void clusterMetadata() {
            final Supplier<MetadataResponseTopic> inklessTopicMetadata =
                () -> new MetadataResponseTopic()
                    .setName(TOPIC_INKLESS)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_INKLESS_ID)
                    .setPartitions(List.of(
                        new MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                    ));

            final List<MetadataResponseTopic> topicMetadata = List.of(inklessTopicMetadata.get());
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            transformer.transformClusterMetadata(null, topicMetadata);
            final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), 0);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);

            transformer.transformClusterMetadata(null, topicMetadata);
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), 1);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
        }

        @Test
        void describeTopicResponse() {
            final Supplier<DescribeTopicPartitionsResponseData> describeResponseSupplier =
                () -> new DescribeTopicPartitionsResponseData()
                    .setTopics(new DescribeTopicPartitionsResponseTopicCollection(List.of(
                        new DescribeTopicPartitionsResponseTopic()
                            .setName(TOPIC_INKLESS)
                            .setErrorCode((short) 0)
                            .setTopicId(TOPIC_INKLESS_ID)
                            .setPartitions(List.of(
                                new DescribeTopicPartitionsResponsePartition()
                                    .setPartitionIndex(0)
                                    .setErrorCode((short) 0)
                                    .setLeaderId(-1)
                                    .setReplicaNodes(List.of(1, 2, 3, 4))
                                    .setIsrNodes(List.of(1, 2))
                                    .setOfflineReplicas(List.of(3, 4))
                                    .setEligibleLeaderReplicas(List.of(10, 11))
                                    .setLastKnownElr(List.of(10, 11))
                            ))
                    ).iterator()));

            final var transformer = new InklessTopicMetadataTransformer(metadataView);
            final DescribeTopicPartitionsResponseData describeResponse = describeResponseSupplier.get();

            transformer.transformDescribeTopicResponse(null, describeResponse);

            final var expectedDescribeResponse = describeResponseSupplier.get();
            setExpectedLeaderDescribeTopicResponse(expectedDescribeResponse.topics().find(TOPIC_INKLESS).partitions().get(0), 0);
            assertThat(describeResponse).isEqualTo(expectedDescribeResponse);

            transformer.transformDescribeTopicResponse(null, describeResponse);
            setExpectedLeaderDescribeTopicResponse(expectedDescribeResponse.topics().find(TOPIC_INKLESS).partitions().get(0), 1);
            assertThat(describeResponse).isEqualTo(expectedDescribeResponse);
        }
    }

    private static void setExpectedLeaderCluster(final MetadataResponsePartition partition, final int leaderId) {
        partition.setLeaderId(leaderId);
        partition.setReplicaNodes(List.of(leaderId));
        partition.setIsrNodes(List.of(leaderId));
        partition.setOfflineReplicas(Collections.emptyList());
    }

    private static void setExpectedLeaderDescribeTopicResponse(final DescribeTopicPartitionsResponsePartition partition, final int leaderId) {
        partition.setLeaderId(leaderId);
        partition.setReplicaNodes(List.of(leaderId));
        partition.setIsrNodes(List.of(leaderId));
        partition.setOfflineReplicas(Collections.emptyList());
        partition.setEligibleLeaderReplicas(Collections.emptyList());
        partition.setLastKnownElr(Collections.emptyList());
    }
}
