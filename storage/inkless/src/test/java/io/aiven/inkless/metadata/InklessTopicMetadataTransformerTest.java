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
package io.aiven.inkless.metadata;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopicCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;

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
import java.util.function.Supplier;

import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class InklessTopicMetadataTransformerTest {
    static final String TOPIC_DISKLESS = "diskless-topic";
    static final Uuid TOPIC_DISKLESS_ID = new Uuid(123, 123);
    static final String TOPIC_CLASSIC = "classic-topic";
    static final Uuid TOPIC_CLASSIC_ID = new Uuid(456, 456);
    static final ListenerName LISTENER_NAME = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);

    @Mock
    MetadataView metadataView;

    @Test
    void nulls() {
        assertThatThrownBy(() -> new InklessTopicMetadataTransformer(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("metadataView cannot be null");

        final var transformer = new InklessTopicMetadataTransformer(metadataView);
        assertThatThrownBy(() -> transformer.transformClusterMetadata(LISTENER_NAME, "x", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicMetadata cannot be null");
        assertThatThrownBy(() -> transformer.transformDescribeTopicResponse(LISTENER_NAME, "x", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("responseData cannot be null");
    }

    @Nested
    class EmptyMetadata {
        @ParameterizedTest
        @NullSource
        @ValueSource(strings = {"diskless_az=az1", "x=y", ""})
        void clusterMetadata(final String clientId) {
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            final List<MetadataResponseTopic> topicMetadata = List.of();
            transformer.transformClusterMetadata(LISTENER_NAME, clientId, topicMetadata);
            assertThat(topicMetadata).isEmpty();
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(strings = {"diskless_az=az1", "x=y", ""})
        void describeTopicResponse(final String clientId) {
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            final DescribeTopicPartitionsResponseData describeResponse = new DescribeTopicPartitionsResponseData();
            transformer.transformDescribeTopicResponse(LISTENER_NAME, clientId, describeResponse);
            assertThat(describeResponse).isEqualTo(new DescribeTopicPartitionsResponseData());
        }
    }

    @Nested
    class InklessAndClassicTopics {
        @BeforeEach
        void setup() {
            when(metadataView.isDisklessTopic(eq(TOPIC_DISKLESS))).thenReturn(true);
            when(metadataView.isDisklessTopic(eq(TOPIC_CLASSIC))).thenReturn(false);
            when(metadataView.getAliveBrokerNodes(LISTENER_NAME)).thenReturn(List.of(
                new Node(0, "host", 9092, "az0"),
                new Node(2, "host", 9094, "az0"),
                new Node(1, "host", 9093, "az1"),
                new Node(3, "host", 9095, "az1")
            ));
        }

        @ParameterizedTest
        @CsvSource({
            "az0,0,0,0",
            "az1,1,1,3",
            "az_unknown,0,3,0",
            ",1,3,1",
        })
        void clusterMetadata(final String clientAZ, final int expectedLeaderId1, final int expectedLeaderId2, final int expectedLeaderId3) {
            final Supplier<MetadataResponseTopic> inklessTopicMetadata =
                () -> new MetadataResponseTopic()
                    .setName(TOPIC_DISKLESS)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_DISKLESS_ID)
                    .setPartitions(List.of(
                        new MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setLeaderEpoch(0),
                        new MetadataResponsePartition()
                            .setPartitionIndex(1)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setLeaderEpoch(0),
                        new MetadataResponsePartition()
                            .setPartitionIndex(2)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setLeaderEpoch(0)
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

            transformer.transformClusterMetadata(LISTENER_NAME, "diskless_az=" + clientAZ, topicMetadata);

            final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), expectedLeaderId1);
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(1), expectedLeaderId2);
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(2), expectedLeaderId3);

            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
            assertThat(topicMetadata.get(1)).isEqualTo(classicTopicMetadata.get());

            // Check that rotation does not happen by transforming again.
            transformer.transformClusterMetadata(LISTENER_NAME, "diskless_az=" + clientAZ, topicMetadata);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
            assertThat(topicMetadata.get(1)).isEqualTo(classicTopicMetadata.get());
        }

        @ParameterizedTest
        @CsvSource({
            "az0,0,0,0",
            "az1,1,1,3",
            "az_unknown,0,3,0",
            ",1,3,1",
        })
        void describeTopicResponse(final String clientAZ, final int expectedLeaderId1, final int expectedLeaderId2, final int expectedLeaderId3) {
            final Supplier<DescribeTopicPartitionsResponseTopic> inklessTopicMetadata =
                () -> new DescribeTopicPartitionsResponseTopic()
                    .setName(TOPIC_DISKLESS)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_DISKLESS_ID)
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
                            .setLeaderEpoch(0),
                        new DescribeTopicPartitionsResponsePartition()
                            .setPartitionIndex(1)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setEligibleLeaderReplicas(List.of(10, 11))
                            .setLastKnownElr(List.of(10, 11))
                            .setLeaderEpoch(0),
                        new DescribeTopicPartitionsResponsePartition()
                            .setPartitionIndex(2)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setEligibleLeaderReplicas(List.of(10, 11))
                            .setLastKnownElr(List.of(10, 11))
                            .setLeaderEpoch(0)
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
                            .setLeaderEpoch(0)
                    ));

            final DescribeTopicPartitionsResponseData describeResponse =
                new DescribeTopicPartitionsResponseData()
                    .setTopics(new DescribeTopicPartitionsResponseTopicCollection(List.of(
                        inklessTopicMetadata.get(),
                        classicTopicMetadata.get()
                    ).iterator()));
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            transformer.transformDescribeTopicResponse(LISTENER_NAME, "diskless_az=" + clientAZ, describeResponse);

            final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
            setExpectedLeaderDescribeTopicResponse(expectedInklessTopicMetadata.partitions().get(0), expectedLeaderId1);
            setExpectedLeaderDescribeTopicResponse(expectedInklessTopicMetadata.partitions().get(1), expectedLeaderId2);
            setExpectedLeaderDescribeTopicResponse(expectedInklessTopicMetadata.partitions().get(2), expectedLeaderId3);

            assertThat(describeResponse.topics().find(TOPIC_DISKLESS)).isEqualTo(expectedInklessTopicMetadata);
            assertThat(describeResponse.topics().find(TOPIC_CLASSIC)).isEqualTo(classicTopicMetadata.get());

            // Check that rotation does not happen by transforming again.
            transformer.transformDescribeTopicResponse(LISTENER_NAME, "diskless_az=" + clientAZ, describeResponse);
            assertThat(describeResponse.topics().find(TOPIC_DISKLESS)).isEqualTo(expectedInklessTopicMetadata);
            assertThat(describeResponse.topics().find(TOPIC_CLASSIC)).isEqualTo(classicTopicMetadata.get());
        }
    }

    @Nested
    class SelectFromAllBrokersWhenBrokerRackIsNotSetCluster {
        @BeforeEach
        void setup() {
            when(metadataView.isDisklessTopic(eq(TOPIC_DISKLESS))).thenReturn(true);
            when(metadataView.getAliveBrokerNodes(LISTENER_NAME)).thenReturn(List.of(
                new Node(1, "host", 9093),
                new Node(0, "host", 9092)
            ));
        }

        @Test
        void clusterMetadata() {
            final Supplier<MetadataResponseTopic> inklessTopicMetadata =
                () -> new MetadataResponseTopic()
                    .setName(TOPIC_DISKLESS)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_DISKLESS_ID)
                    .setPartitions(List.of(
                        new MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setLeaderEpoch(0)
                    ));

            final List<MetadataResponseTopic> topicMetadata = List.of(inklessTopicMetadata.get());
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            transformer.transformClusterMetadata(LISTENER_NAME, "diskless_az=az0", topicMetadata);
            final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), 0);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);

            transformer.transformClusterMetadata(LISTENER_NAME, "diskless_az=az0", topicMetadata);
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), 0);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
        }

        @Test
        void describeTopicResponse() {
            final Supplier<DescribeTopicPartitionsResponseData> describeResponseSupplier =
                () -> new DescribeTopicPartitionsResponseData()
                    .setTopics(new DescribeTopicPartitionsResponseTopicCollection(List.of(
                        new DescribeTopicPartitionsResponseTopic()
                            .setName(TOPIC_DISKLESS)
                            .setErrorCode((short) 0)
                            .setTopicId(TOPIC_DISKLESS_ID)
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
                                    .setLeaderEpoch(0)
                            ))
                    ).iterator()));

            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            final DescribeTopicPartitionsResponseData describeResponse = describeResponseSupplier.get();
            transformer.transformDescribeTopicResponse(LISTENER_NAME, "diskless_az=az0", describeResponse);

            final var expectedDescribeResponse = describeResponseSupplier.get();
            setExpectedLeaderDescribeTopicResponse(expectedDescribeResponse.topics().find(TOPIC_DISKLESS).partitions().get(0), 0);
            assertThat(describeResponse).isEqualTo(expectedDescribeResponse);

            transformer.transformDescribeTopicResponse(LISTENER_NAME, "diskless_az=az0", describeResponse);
            setExpectedLeaderDescribeTopicResponse(expectedDescribeResponse.topics().find(TOPIC_DISKLESS).partitions().get(0), 0);
            assertThat(describeResponse).isEqualTo(expectedDescribeResponse);
        }
    }

    @Nested
    class SelectFromAllBrokersWhenClientAZIsNotSetCluster {
        @BeforeEach
        void setup() {
            when(metadataView.isDisklessTopic(eq(TOPIC_DISKLESS))).thenReturn(true);
            when(metadataView.getAliveBrokerNodes(LISTENER_NAME)).thenReturn(List.of(
                new Node(1, "host", 9093, "az1"),
                new Node(0, "host", 9092, "az0")
            ));
        }

        @Test
        void clusterMetadata() {
            final Supplier<MetadataResponseTopic> inklessTopicMetadata =
                () -> new MetadataResponseTopic()
                    .setName(TOPIC_DISKLESS)
                    .setErrorCode((short) 0)
                    .setTopicId(TOPIC_DISKLESS_ID)
                    .setPartitions(List.of(
                        new MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setLeaderId(-1)
                            .setReplicaNodes(List.of(1, 2, 3, 4))
                            .setIsrNodes(List.of(1, 2))
                            .setOfflineReplicas(List.of(3, 4))
                            .setLeaderEpoch(0)
                    ));

            final List<MetadataResponseTopic> topicMetadata = List.of(inklessTopicMetadata.get());
            final var transformer = new InklessTopicMetadataTransformer(metadataView);

            transformer.transformClusterMetadata(LISTENER_NAME, null, topicMetadata);
            final var expectedInklessTopicMetadata = inklessTopicMetadata.get();
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), 1);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);

            transformer.transformClusterMetadata(LISTENER_NAME, null, topicMetadata);
            setExpectedLeaderCluster(expectedInklessTopicMetadata.partitions().get(0), 1);
            assertThat(topicMetadata.get(0)).isEqualTo(expectedInklessTopicMetadata);
        }

        @Test
        void describeTopicResponse() {
            final Supplier<DescribeTopicPartitionsResponseData> describeResponseSupplier =
                () -> new DescribeTopicPartitionsResponseData()
                    .setTopics(new DescribeTopicPartitionsResponseTopicCollection(List.of(
                        new DescribeTopicPartitionsResponseTopic()
                            .setName(TOPIC_DISKLESS)
                            .setErrorCode((short) 0)
                            .setTopicId(TOPIC_DISKLESS_ID)
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
                                    .setLeaderEpoch(0)
                            ))
                    ).iterator()));

            final var transformer = new InklessTopicMetadataTransformer(metadataView);
            final DescribeTopicPartitionsResponseData describeResponse = describeResponseSupplier.get();

            transformer.transformDescribeTopicResponse(LISTENER_NAME, null, describeResponse);

            final var expectedDescribeResponse = describeResponseSupplier.get();
            setExpectedLeaderDescribeTopicResponse(expectedDescribeResponse.topics().find(TOPIC_DISKLESS).partitions().get(0), 1);
            assertThat(describeResponse).isEqualTo(expectedDescribeResponse);

            transformer.transformDescribeTopicResponse(LISTENER_NAME, null, describeResponse);
            setExpectedLeaderDescribeTopicResponse(expectedDescribeResponse.topics().find(TOPIC_DISKLESS).partitions().get(0), 1);
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
