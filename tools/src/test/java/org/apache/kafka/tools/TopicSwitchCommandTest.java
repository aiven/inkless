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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterDisklessSwitchOptions;
import org.apache.kafka.clients.admin.AlterDisklessSwitchResult;
import org.apache.kafka.clients.admin.DescribeTopicPartitionsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.metadata.InitDisklessLogFields;
import org.apache.kafka.metadata.InitDisklessLogFields.ProducerStateEntry;
import org.apache.kafka.metadata.PartitionRegistration;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TopicSwitchCommandTest {

    private static final String TOPIC = "test-topic";

    @Mock
    private Admin adminClient;

    @Test
    public void testStateCommandFullySwitched() throws Exception {
        DescribeTopicPartitionsResponseData responseData = buildResponseData(
            List.of(buildPartition(0, 1, 5, 100L, 7), buildPartition(1, 2, 6, 200L, 8))
        );
        mockAdminCalls(responseData, Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(150, -1, java.util.Optional.empty()),
            new TopicPartition(TOPIC, 1), new ListOffsetsResult.ListOffsetsResultInfo(250, -1, java.util.Optional.empty())
        ), Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty()),
            new TopicPartition(TOPIC, 1), new ListOffsetsResult.ListOffsetsResultInfo(10, -1, java.util.Optional.empty())
        ));

        String output = runStateCommand();
        assertTrue(output.contains("Partition 0:"));
        assertTrue(output.contains("leader=1 epoch=5"));
        assertTrue(output.contains("classicToDisklessStartOffset=100"));
        assertTrue(output.contains("disklessLeaderEpoch=7"));
        assertTrue(output.contains("logStartOffset=0 logEndOffset=150"));

        assertTrue(output.contains("Partition 1:"));
        assertTrue(output.contains("leader=2 epoch=6"));
        assertTrue(output.contains("classicToDisklessStartOffset=200"));
        assertTrue(output.contains("disklessLeaderEpoch=8"));
        assertTrue(output.contains("logStartOffset=10 logEndOffset=250"));
    }

    @Test
    public void testStateCommandNotSwitched() throws Exception {
        DescribeTopicPartitionsResponseData responseData = buildResponseData(
            List.of(buildPartition(0, 1, 3, -1L, -1))
        );
        mockAdminCalls(responseData, Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(50, -1, java.util.Optional.empty())
        ), Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty())
        ));

        String output = runStateCommand();
        assertTrue(output.contains("classicToDisklessStartOffset=-1 (not switched)"));
        assertTrue(output.contains("disklessLeaderEpoch=-1 (none)"));
        assertTrue(output.contains("  classicPrefixInRemoteStorage=false" + System.lineSeparator()));
    }

    @Test
    public void testStateCommandSwitchPending() throws Exception {
        DescribeTopicPartitionsResponseData responseData = buildResponseData(
            List.of(buildPartition(0, 1, 4, -2L, -1))
        );
        mockAdminCalls(responseData, Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(75, -1, java.util.Optional.empty())
        ), Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty())
        ));

        String output = runStateCommand();

        assertTrue(output.contains("classicToDisklessStartOffset=-2 (switch pending)"));
        assertTrue(output.contains("  classicPrefixInRemoteStorage=false" + System.lineSeparator()));
    }

    @Test
    public void testStateCommandShowsPhase1WhenSealCommitted() throws Exception {
        DescribeTopicPartitionsResponseData responseData = buildResponseData(
            List.of(buildPartition(0, 1, 5, 100L, 5, false))
        );
        mockAdminCalls(responseData, Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(150, -1, java.util.Optional.empty())
        ), Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty())
        ));

        String output = runStateCommand();

        assertTrue(output.contains("classicPrefixInRemoteStorage=false (phase 1)"));
    }

    @Test
    public void testStateCommandShowsPhase2WhenFlagSet() throws Exception {
        DescribeTopicPartitionsResponsePartition phaseTwo = buildPartition(0, 1, 6, 100L, 6, true);
        phaseTwo.unknownTaggedFields().add(InitDisklessLogFields.encodeLastClassicLeaderEpoch(4));
        DescribeTopicPartitionsResponseData responseData = buildResponseData(
            List.of(phaseTwo, buildPartition(1, 2, 5, 200L, 5, false))
        );
        mockAdminCalls(responseData, Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(150, -1, java.util.Optional.empty()),
            new TopicPartition(TOPIC, 1), new ListOffsetsResult.ListOffsetsResultInfo(250, -1, java.util.Optional.empty())
        ), Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty()),
            new TopicPartition(TOPIC, 1), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty())
        ));

        String output = runStateCommand();

        int partition1 = output.indexOf("Partition 1:");
        assertTrue(partition1 > 0, output);
        assertTrue(output.substring(0, partition1).contains("classicPrefixInRemoteStorage=true (phase 2)"), output);
        assertTrue(output.substring(0, partition1).contains("lastClassicLeaderEpoch=4" + System.lineSeparator()), output);
        assertTrue(output.substring(partition1).contains("classicPrefixInRemoteStorage=false (phase 1)"), output);
        assertTrue(output.substring(partition1).contains("lastClassicLeaderEpoch=-1 (none)"), output);
    }

    @Test
    public void testStateCommandWithProducerStates() throws Exception {
        List<ProducerStateEntry> producerStates = List.of(
            new ProducerStateEntry(1001L, (short) 5, 0, 10, 99L, 1700000000000L),
            new ProducerStateEntry(2002L, (short) 3, 5, 15, 150L, 1700000001000L)
        );

        DescribeTopicPartitionsResponsePartition partition = buildPartition(0, 1, 10, 500L, 12);
        partition.unknownTaggedFields().add(InitDisklessLogFields.encodeProducerStates(producerStates));

        DescribeTopicPartitionsResponseData responseData = buildResponseData(List.of(partition));
        mockAdminCalls(responseData, Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(600, -1, java.util.Optional.empty())
        ), Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty())
        ));

        String output = runStateCommand();
        assertTrue(output.contains("producerStates (2):"));
        assertTrue(output.contains("producerId=1001 epoch=5 seq=0..10 offset=99 timestamp=1700000000000"));
        assertTrue(output.contains("producerId=2002 epoch=3 seq=5..15 offset=150 timestamp=1700000001000"));
    }

    @Test
    public void testStateCommandTopicNotFound() {
        DescribeTopicPartitionsResponseData responseData = new DescribeTopicPartitionsResponseData();

        DescribeTopicPartitionsResult describeResult = new DescribeTopicPartitionsResult(
            KafkaFuture.completedFuture(responseData));
        when(adminClient.describeTopicPartitions(ArgumentMatchers.any(), any(DescribeTopicsOptions.class)))
            .thenReturn(describeResult);

        PrintStream stream = new PrintStream(new ByteArrayOutputStream());
        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> TopicSwitchCommand.stateCommand(stream, adminClient, TOPIC));
        assertTrue(ex.getMessage().contains("Topic not found"));
    }

    @Test
    public void testStateCommandMixedPartitions() throws Exception {
        DescribeTopicPartitionsResponseData responseData = buildResponseData(
            List.of(buildPartition(0, 1, 5, 100L, 7), buildPartition(1, 2, 6, -1L, -1))
        );
        mockAdminCalls(responseData, Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(150, -1, java.util.Optional.empty()),
            new TopicPartition(TOPIC, 1), new ListOffsetsResult.ListOffsetsResultInfo(50, -1, java.util.Optional.empty())
        ), Map.of(
            new TopicPartition(TOPIC, 0), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty()),
            new TopicPartition(TOPIC, 1), new ListOffsetsResult.ListOffsetsResultInfo(0, -1, java.util.Optional.empty())
        ));

        String output = runStateCommand();
        assertTrue(output.contains("classicToDisklessStartOffset=100"));
        assertTrue(output.contains("classicToDisklessStartOffset=-1 (not switched)"));
    }

    @Test
    public void testRearmSendsReArmOffset() throws Exception {
        mockAlterDisklessSwitch();

        String output = runRearmCommand(0);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0),
            eq(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING), any(AlterDisklessSwitchOptions.class));
        assertTrue(output.contains("Re-armed the switch of test-topic-0."), output);
        // Controllers don't serve DescribeTopicPartitions, so rearm must work with --bootstrap-controller.
        verify(adminClient, never()).describeTopicPartitions(any(), any(DescribeTopicsOptions.class));
    }

    @Test
    public void testRearmAcceptsBootstrapController() throws Exception {
        TopicSwitchCommand.argumentParser()
            .parseArgs(new String[]{"rearm", "--bootstrap-controller", "localhost:9093", "--topic", TOPIC, "--partition", "0"});
    }

    @Test
    public void testRearmRequiresPartition() {
        assertThrows(ArgumentParserException.class, () -> TopicSwitchCommand.argumentParser()
            .parseArgs(new String[]{"rearm", "--bootstrap-server", "localhost:9092", "--topic", TOPIC}));
    }

    @ParameterizedTest
    @ValueSource(strings = {"seal", "repair"})
    public void testRemovedCommandsAreRejected(String command) {
        assertThrows(ArgumentParserException.class, () -> TopicSwitchCommand.argumentParser()
            .parseArgs(new String[]{command, "--bootstrap-server", "localhost:9092", "--topic", TOPIC, "--partition", "0"}));
    }

    private void mockAlterDisklessSwitch() {
        AlterDisklessSwitchResult result = mock(AlterDisklessSwitchResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.alterDisklessSwitch(any(), anyInt(), anyLong(), any(AlterDisklessSwitchOptions.class)))
            .thenReturn(result);
    }

    private String runRearmCommand(int partition) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, false, StandardCharsets.UTF_8);
        TopicSwitchCommand.rearmCommand(stream, adminClient, TOPIC, partition);
        return out.toString(StandardCharsets.UTF_8);
    }

    private String runStateCommand() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, false, StandardCharsets.UTF_8);
        TopicSwitchCommand.stateCommand(stream, adminClient, TOPIC);
        return out.toString(StandardCharsets.UTF_8);
    }

    private DescribeTopicPartitionsResponseData buildResponseData(List<DescribeTopicPartitionsResponsePartition> partitions) {
        DescribeTopicPartitionsResponseData responseData = new DescribeTopicPartitionsResponseData();
        DescribeTopicPartitionsResponseTopic topicData = new DescribeTopicPartitionsResponseTopic()
            .setName(TOPIC)
            .setErrorCode((short) 0);
        topicData.partitions().addAll(partitions);
        responseData.topics().add(topicData);
        return responseData;
    }

    private DescribeTopicPartitionsResponsePartition buildPartition(
            int partitionIndex, int leaderId, int leaderEpoch,
            long classicToDisklessStartOffset, int disklessLeaderEpoch) {
        return buildPartition(partitionIndex, leaderId, leaderEpoch, classicToDisklessStartOffset, disklessLeaderEpoch, false);
    }

    private DescribeTopicPartitionsResponsePartition buildPartition(
            int partitionIndex, int leaderId, int leaderEpoch,
            long classicToDisklessStartOffset, int disklessLeaderEpoch, boolean classicPrefixInRemoteStorage) {
        DescribeTopicPartitionsResponsePartition partition = new DescribeTopicPartitionsResponsePartition()
            .setPartitionIndex(partitionIndex)
            .setLeaderId(leaderId)
            .setLeaderEpoch(leaderEpoch)
            .setReplicaNodes(List.of(leaderId))
            .setIsrNodes(List.of(leaderId));

        if (classicToDisklessStartOffset >= 0 || classicToDisklessStartOffset == -2) {
            partition.unknownTaggedFields().add(
                InitDisklessLogFields.encodeClassicToDisklessStartOffset(classicToDisklessStartOffset));
        }
        if (disklessLeaderEpoch >= 0) {
            partition.unknownTaggedFields().add(
                InitDisklessLogFields.encodeDisklessLeaderEpoch(disklessLeaderEpoch));
        }
        if (classicPrefixInRemoteStorage) {
            partition.unknownTaggedFields().add(InitDisklessLogFields.encodeClassicPrefixInRemoteStorage());
        }

        return partition;
    }

    private void mockAdminCalls(
            DescribeTopicPartitionsResponseData responseData,
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets,
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets) {
        DescribeTopicPartitionsResult describeResult = new DescribeTopicPartitionsResult(
            KafkaFuture.completedFuture(responseData));
        when(adminClient.describeTopicPartitions(ArgumentMatchers.any(), any(DescribeTopicsOptions.class)))
            .thenReturn(describeResult);

        when(adminClient.listOffsets(anyMap()))
            .thenAnswer(invocation -> {
                Map<TopicPartition, OffsetSpec> request = invocation.getArgument(0);
                boolean isLatest = request.values().stream().anyMatch(s -> s instanceof OffsetSpec.LatestSpec);
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> results = isLatest ? latestOffsets : earliestOffsets;

                Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = new java.util.HashMap<>();
                for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : results.entrySet()) {
                    futures.put(entry.getKey(), KafkaFuture.completedFuture(entry.getValue()));
                }
                return new ListOffsetsResult(futures);
            });
    }
}
