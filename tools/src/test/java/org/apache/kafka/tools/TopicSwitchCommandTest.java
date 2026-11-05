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
import org.apache.kafka.clients.admin.DescribeDisklessSwitchReplicaResult;
import org.apache.kafka.clients.admin.DescribeTopicPartitionsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DisklessSwitchReplicaDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RepairDisklessLogResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.metadata.InitDisklessLogFields;
import org.apache.kafka.metadata.InitDisklessLogFields.ProducerStateEntry;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TopicSwitchCommandTest {

    private static final String TOPIC = "test-topic";
    private static final long SEAL = 100L;
    // Phase 1 replicas of a partition sealed at 100, whose remote copy holds [0, 60).
    private static final DisklessSwitchReplicaDescription WIPED = replica(true, 0, 0, 0, 0, 0, 60);
    private static final DisklessSwitchReplicaDescription COMPLETE = replica(false, 0, SEAL, SEAL, 0, 0, 60);
    private static final DisklessSwitchReplicaDescription LAGGING = replica(false, 0, 80, 80, 0, 0, 60);

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
        DescribeTopicPartitionsResponseData responseData = buildResponseData(
            List.of(buildPartition(0, 1, 6, 100L, 6, true), buildPartition(1, 2, 5, 200L, 5, false))
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
        assertTrue(output.substring(partition1).contains("classicPrefixInRemoteStorage=false (phase 1)"), output);
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
    public void testSealCommitsExplicitOffset() throws Exception {
        mockDescribeSeal(0, 120L);
        mockLogRange(0, 10, 150);
        mockAlterDisklessSwitch();

        String output = runSealCommand(0, Optional.of(100L), false);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0), eq(100L), any(AlterDisklessSwitchOptions.class));
        assertTrue(output.contains("within classic log range [10, 150]"));
        assertTrue(output.contains("Set test-topic-0 classicToDisklessStartOffset to 100"));
    }

    @Test
    public void testSealRejectsExplicitOffsetAboveEndOffset() throws Exception {
        mockDescribeSeal(0, -2L);
        mockLogRange(0, 0, 90);

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.of(100L), false));
        assertTrue(ex.getMessage().contains("outside the classic log range [0, 90]"));
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testSealRejectsExplicitOffsetBelowStartOffset() throws Exception {
        mockDescribeSeal(0, -2L);
        mockLogRange(0, 50, 150);

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.of(20L), false));
        assertTrue(ex.getMessage().contains("outside the classic log range [50, 150]"));
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testSealDefaultsToEndOffsetWhenPending() throws Exception {
        // Pending switch (offset -2): default seals at the current end offset.
        mockDescribeSeal(0, -2L);
        mockLogRange(0, 0, 150);
        mockAlterDisklessSwitch();

        String output = runSealCommand(0, Optional.empty(), false);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0), eq(150L), any(AlterDisklessSwitchOptions.class));
        assertTrue(output.contains("Set test-topic-0 classicToDisklessStartOffset to 150"));
    }

    @Test
    public void testSealDefaultRejectedWhenAlreadyCommitted() throws Exception {
        // Already committed (offset 100): defaulting would use the diskless end offset, so require --offset.
        mockDescribeSeal(0, 100L);

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.empty(), false));
        assertTrue(ex.getMessage().contains("already has a committed seal offset (100)"));
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testSealDryRunDoesNotCommit() throws Exception {
        mockDescribeSeal(0, 120L);
        mockLogRange(0, 0, 150);

        String output = runSealCommand(0, Optional.of(100L), true);

        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
        assertTrue(output.contains("[dry-run]"));
        assertTrue(output.contains("Would set test-topic-0 classicToDisklessStartOffset to 100"));
    }

    @Test
    public void testSealRejectsExplicitOffsetAboveCommittedSeal() throws Exception {
        mockDescribeSeal(0, 100L);

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.of(120L), false));
        assertTrue(ex.getMessage().contains("exceeds the committed seal offset (100)"), ex.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testSealDryRunRejectsExplicitOffsetAboveCommittedSeal() throws Exception {
        mockDescribeSeal(0, 100L);

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.of(120L), true));
        assertTrue(ex.getMessage().contains("exceeds the committed seal offset (100)"), ex.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testSealCommitsExplicitOffsetEqualToCommittedSeal() throws Exception {
        mockDescribeSeal(0, 100L);
        mockLogRange(0, 0, 150);
        mockAlterDisklessSwitch();

        runSealCommand(0, Optional.of(100L), false);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0), eq(100L), any(AlterDisklessSwitchOptions.class));
    }

    @Test
    public void testSealRejectsExplicitOffsetWhenClassicPrefixInRemoteStorage() throws Exception {
        mockDescribeSeal(0, 100L, true);

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.of(90L), false));
        assertTrue(ex.getMessage().contains("classic prefix in remote storage"), ex.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testSealDryRunRejectsWhenClassicPrefixInRemoteStorage() throws Exception {
        mockDescribeSeal(0, 100L, true);

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.of(90L), true));
        assertTrue(ex.getMessage().contains("classic prefix in remote storage"), ex.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testSealDefaultRejectsWhenClassicPrefixInRemoteStorage() throws Exception {
        mockDescribeSeal(0, 100L, true);

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.empty(), false));
        assertTrue(ex.getMessage().contains("classic prefix in remote storage"), ex.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testSealAbortDoesNotCheckEndOffset() throws Exception {
        mockAlterDisklessSwitch();

        String output = runSealCommand(0, Optional.of(-1L), false);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0), eq(-1L), any(AlterDisklessSwitchOptions.class));
        assertTrue(output.contains("-1 (abort switch, revert to classic)"));
        assertFalse(output.contains("end offset"));
        // Controllers don't serve DescribeTopicPartitions, so the abort must work with --bootstrap-controller.
        verify(adminClient, never()).describeTopicPartitions(any(), any(DescribeTopicsOptions.class));
    }

    @Test
    public void testSealReArm() throws Exception {
        mockAlterDisklessSwitch();

        String output = runSealCommand(0, Optional.of(-2L), false);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0), eq(-2L), any(AlterDisklessSwitchOptions.class));
        assertTrue(output.contains("-2 (re-arm switch)"));
        verify(adminClient, never()).describeTopicPartitions(any(), any(DescribeTopicsOptions.class));
    }

    @Test
    public void testSealRejectsInvalidOffset() {
        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runSealCommand(0, Optional.of(-3L), false));
        assertTrue(ex.getMessage().contains("Invalid seal offset -3"));
    }

    @Test
    public void testRepairSinglePartition() throws Exception {
        mockDescribe(List.of(buildPartition(0, 3, 5, 100L, 7)));
        mockRepairDisklessLog();

        String output = runRepairCommand(Optional.of(0));

        verify(adminClient).repairDisklessLog(eq(TOPIC), eq(0), eq(3));
        assertTrue(output.contains("Repaired test-topic-0 control-plane diskless log at seal offset 100 (leader 3)"));
    }

    @Test
    public void testRepairAllPartitionsSkipsUnswitched() throws Exception {
        // Partition 0 committed (seal 100), partition 1 not switched (-1).
        mockDescribe(List.of(buildPartition(0, 3, 5, 100L, 7), buildPartition(1, 4, 6, -1L, -1)));
        mockRepairDisklessLog();

        String output = runRepairCommand(Optional.empty());

        verify(adminClient).repairDisklessLog(eq(TOPIC), eq(0), eq(3));
        verify(adminClient, never()).repairDisklessLog(eq(TOPIC), eq(1), anyInt());
        assertTrue(output.contains("Skipping test-topic-1: not switched"));
        assertTrue(output.contains("Repaired 1 partition(s)"));
    }

    @Test
    public void testRepairRejectsUnknownPartition() throws Exception {
        mockDescribe(List.of(buildPartition(0, 3, 5, 100L, 7)));

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runRepairCommand(Optional.of(5)));
        assertTrue(ex.getMessage().contains("Partition not found: test-topic-5"));
        verify(adminClient, never()).repairDisklessLog(any(), anyInt(), anyInt());
    }

    @Test
    public void testRepairFailsOnTopicError() throws Exception {
        DescribeTopicPartitionsResponseData responseData = buildResponseData(List.of(buildPartition(0, 3, 5, 100L, 7)));
        responseData.topics().find(TOPIC).setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
        when(adminClient.describeTopicPartitions(ArgumentMatchers.any(), any(DescribeTopicsOptions.class)))
            .thenReturn(new DescribeTopicPartitionsResult(KafkaFuture.completedFuture(responseData)));

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> runRepairCommand(Optional.of(0)));
        assertTrue(ex.getMessage().contains("Error describing topic " + TOPIC));
        verify(adminClient, never()).repairDisklessLog(any(), anyInt(), anyInt());
    }

    @Test
    public void testRepairSkipsPartitionWithoutLeader() throws Exception {
        mockDescribe(List.of(buildPartition(0, -1, 5, 100L, 7)));

        String output = runRepairCommand(Optional.of(0));

        verify(adminClient, never()).repairDisklessLog(any(), anyInt(), anyInt());
        assertTrue(output.contains("Skipping test-topic-0: no current leader"));
    }

    @Test
    public void testRepairRejectsBootstrapController() {
        assertThrows(ArgumentParserException.class, () -> TopicSwitchCommand.argumentParser()
            .parseArgs(new String[]{"repair", "--bootstrap-controller", "localhost:9093", "--topic", TOPIC}));
    }

    @Test
    public void testRepairAcceptsBootstrapServer() throws Exception {
        TopicSwitchCommand.argumentParser()
            .parseArgs(new String[]{"repair", "--bootstrap-server", "localhost:9092", "--topic", TOPIC});
    }

    @Test
    public void testSealAcceptsBootstrapController() throws Exception {
        TopicSwitchCommand.argumentParser()
            .parseArgs(new String[]{"seal", "--bootstrap-controller", "localhost:9093", "--topic", TOPIC, "--partition", "0"});
    }

    @Test
    public void testRecoveryCommandsRejectBootstrapController() {
        for (String command : List.of("describe", "elect", "start-phase-2")) {
            assertThrows(ArgumentParserException.class, () -> TopicSwitchCommand.argumentParser().parseArgs(
                new String[]{command, "--bootstrap-controller", "localhost:9093", "--topic", TOPIC, "--partition", "0",
                    "--replica", "2"}), command);
        }
    }

    @Test
    public void testRecoveryCommandsParse() throws Exception {
        TopicSwitchCommand.argumentParser().parseArgs(new String[]{"describe", "--bootstrap-server", "localhost:9092",
            "--topic", TOPIC, "--partition", "0"});
        TopicSwitchCommand.argumentParser().parseArgs(new String[]{"elect", "--bootstrap-server", "localhost:9092",
            "--topic", TOPIC, "--partition", "0", "--replica", "2", "--dry-run"});
        TopicSwitchCommand.argumentParser().parseArgs(new String[]{"start-phase-2", "--bootstrap-server",
            "localhost:9092", "--topic", TOPIC, "--partition", "0", "--accept-data-loss", "--dry-run"});
    }

    @Test
    public void testElectRequiresReplica() {
        assertThrows(ArgumentParserException.class, () -> TopicSwitchCommand.argumentParser().parseArgs(
            new String[]{"elect", "--bootstrap-server", "localhost:9092", "--topic", TOPIC, "--partition", "0"}));
    }

    @Test
    public void testElectReplicaHoldingClassicPrefix() throws Exception {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, COMPLETE);
        mockReplica(3, LAGGING);
        mockAlterDisklessSwitch();

        String output = runElectCommand(2, false);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0), eq(SEAL),
            argThat(options -> options.leaderId() == 2 && !options.classicPrefixInRemoteStorage()));
        assertTrue(output.contains("broker 2 holds the classic prefix up to the seal 100"), output);
        assertTrue(output.contains("Elected broker 2 as the leader of test-topic-0"), output);
    }

    @Test
    public void testElectDryRunDoesNotCommit() throws Exception {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, COMPLETE);
        mockReplica(3, LAGGING);

        String output = runElectCommand(2, true);

        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
        assertTrue(output.contains("[dry-run] Would elect broker 2 as the leader of test-topic-0"), output);
    }

    @Test
    public void testElectAcceptsReplicaWhoseLogContinuesRemoteCopy() throws Exception {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, replica(false, 50, 100, 100, 0, 0, 60));
        mockReplica(3, LAGGING);

        String output = runElectCommand(2, true);

        assertTrue(output.contains("[dry-run] Would elect broker 2"), output);
    }

    @Test
    public void testElectRejectsReplicaBelowSeal() {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, COMPLETE);
        mockReplica(3, LAGGING);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runElectCommand(3, false));

        assertTrue(ex.getMessage().contains("its high watermark 80 is below the seal"), ex.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testElectRejectsReplicaWithGapAfterRemoteCopy() {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, replica(false, 70, 100, 100, 0, 0, 60));
        mockReplica(3, LAGGING);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runElectCommand(2, true));

        assertTrue(ex.getMessage().contains("its log starts at 70, and remote storage holds the prefix only up to 60"),
            ex.getMessage());
    }

    @Test
    public void testElectRejectsReplicaWhenRemoteCopyMissesStart() {
        mockPhaseOnePartition();
        mockReplica(1, replica(true, 0, 0, 0, 0, 20, 60));
        mockReplica(2, replica(false, 30, 100, 100, 0, 20, 60));
        mockReplica(3, replica(false, 0, 80, 80, 0, 20, 60));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runElectCommand(2, true));

        assertTrue(ex.getMessage().contains("remote storage misses the offsets from 0 to 20"), ex.getMessage());
    }

    @Test
    public void testElectRejectsCurrentLeaderAndNonReplica() {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, COMPLETE);
        mockReplica(3, LAGGING);

        RuntimeException leader = assertThrows(RuntimeException.class, () -> runElectCommand(1, false));
        assertTrue(leader.getMessage().contains("already the leader"), leader.getMessage());
        RuntimeException notReplica = assertThrows(RuntimeException.class, () -> runElectCommand(4, false));
        assertTrue(notReplica.getMessage().contains("isn't a replica"), notReplica.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testElectRejectsUnreachableReplica() {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplicaError(2, new TimeoutException("Timed out waiting for broker 2"));
        mockReplica(3, LAGGING);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runElectCommand(2, false));

        assertTrue(ex.getMessage().contains("Cannot describe broker 2's replica"), ex.getMessage());
        assertTrue(ex.getMessage().contains("Timed out waiting for broker 2"), ex.getMessage());
    }

    @Test
    public void testElectRejectsPartitionOutsidePhaseOne() {
        mockDescribe(List.of(buildPartition(0, 1, 7, SEAL, 7, true)));
        mockReplica(1, COMPLETE);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runElectCommand(2, false));

        assertTrue(ex.getMessage().contains("isn't in phase 1 of the switch"), ex.getMessage());
    }

    @Test
    public void testStartPhaseTwoRefusesWhenReplicaHoldsClassicPrefix() {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, COMPLETE);
        mockReplica(3, LAGGING);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runStartPhaseTwoCommand(true, false));

        assertTrue(ex.getMessage().contains("Broker 2 holds the whole classic prefix"), ex.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testStartPhaseTwoRefusesWhenLeaderHoldsClassicPrefix() {
        mockPhaseOnePartition();
        mockReplica(1, COMPLETE);
        mockReplica(2, LAGGING);
        mockReplica(3, LAGGING);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runStartPhaseTwoCommand(true, false));

        assertTrue(ex.getMessage().contains("doesn't need recovery"), ex.getMessage());
    }

    @Test
    public void testStartPhaseTwoRequiresAcceptDataLoss() {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, LAGGING);
        mockReplica(3, LAGGING);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runStartPhaseTwoCommand(false, false));

        assertTrue(ex.getMessage().contains("at seal 60 loses the records in [60, 100)"), ex.getMessage());
        assertTrue(ex.getMessage().contains("--accept-data-loss"), ex.getMessage());
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testStartPhaseTwoWithAcceptDataLossLowersSeal() throws Exception {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, LAGGING);
        mockReplica(3, LAGGING);
        mockAlterDisklessSwitch();

        String output = runStartPhaseTwoCommand(true, false);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0), eq(60L),
            argThat(options -> options.classicPrefixInRemoteStorage() && options.leaderId() == -1));
        assertTrue(output.contains("Accepting the loss of the records in [60, 100)"), output);
        assertTrue(output.contains("Started phase 2 for test-topic-0 at seal 60"), output);
        assertTrue(output.contains("Run the repair command"), output);
    }

    @Test
    public void testStartPhaseTwoWithoutLossWhenRemoteHoldsWholePrefix() throws Exception {
        mockPhaseOnePartition();
        mockReplica(1, replica(true, 0, 0, 0, 0, 0, SEAL));
        mockReplica(2, LAGGING);
        mockReplica(3, LAGGING);
        mockAlterDisklessSwitch();

        String output = runStartPhaseTwoCommand(false, false);

        verify(adminClient).alterDisklessSwitch(eq(TOPIC), eq(0), eq(SEAL),
            argThat(AlterDisklessSwitchOptions::classicPrefixInRemoteStorage));
        assertFalse(output.contains("Accepting the loss"), output);
        assertFalse(output.contains("Run the repair command"), output);
    }

    @Test
    public void testStartPhaseTwoRequiresAcceptDataLossForMissingStart() {
        mockPhaseOnePartition();
        mockReplica(1, replica(true, 0, 0, 0, 0, 20, SEAL));
        mockReplica(2, LAGGING);
        mockReplica(3, LAGGING);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runStartPhaseTwoCommand(false, false));

        assertTrue(ex.getMessage().contains("loses the records in [0, 20)"), ex.getMessage());
    }

    @Test
    public void testStartPhaseTwoUsesHighestReportedRemoteEnd() throws Exception {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, replica(false, 0, 80, 80, 0, 0, 40));
        mockReplica(3, replica(false, 0, 80, 80, 0, 0, 90));

        String output = runStartPhaseTwoCommand(true, true);

        assertTrue(output.contains("[dry-run] Would start phase 2 for test-topic-0 at seal 90"), output);
        verify(adminClient, never()).alterDisklessSwitch(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void testStartPhaseTwoRefusesWithoutRemoteRange() {
        mockPhaseOnePartition();
        DisklessSwitchReplicaDescription unknownRemote = replica(false, 0, 80, 80, -1, -1, -1);
        mockReplica(1, replica(true, 0, 0, 0, -1, -1, -1));
        mockReplica(2, unknownRemote);
        mockReplica(3, unknownRemote);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> runStartPhaseTwoCommand(true, false));

        assertTrue(ex.getMessage().contains("Retry once the remote log manager is ready"), ex.getMessage());
    }

    @Test
    public void testStartPhaseTwoWarnsAboutUnreachableReplica() throws Exception {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, LAGGING);
        mockReplicaError(3, new TimeoutException("Timed out waiting for broker 3"));

        String output = runStartPhaseTwoCommand(true, true);

        assertTrue(output.contains("Warning: broker 3's replica isn't checked"), output);
        assertTrue(output.contains("[dry-run] Would start phase 2 for test-topic-0 at seal 60"), output);
    }

    @Test
    public void testDescribeCommandReportsEachReplica() throws Exception {
        mockPhaseOnePartition();
        mockReplica(1, WIPED);
        mockReplica(2, COMPLETE);
        mockReplicaError(3, new TimeoutException("Timed out waiting for broker 3"));

        String output = runDescribeCommand();

        assertTrue(output.contains("leader=1 epoch=7 isr=[1] replicas=[1, 2, 3]"), output);
        assertTrue(output.contains("crossTierLogStartOffset=0 remoteStartOffset=0 remoteEndOffset=60"), output);
        assertTrue(output.contains("Broker 1: offline=true logStartOffset=0 logEndOffset=0 highWatermark=0"), output);
        assertTrue(output.contains("the broker took the partition offline"), output);
        assertTrue(output.contains("Broker 2: offline=false logStartOffset=0 logEndOffset=100 highWatermark=100"),
            output);
        assertTrue(output.contains("Holds the whole classic prefix"), output);
        assertTrue(output.contains("Broker 3: org.apache.kafka.common.errors.TimeoutException"), output);
        assertTrue(output.contains("start-phase-2 would set the seal to 60. It would lose [60, 100)."), output);
    }

    private static DisklessSwitchReplicaDescription replica(boolean offline, long logStartOffset, long logEndOffset,
                                                            long highWatermark, long crossTierLogStartOffset,
                                                            long remoteStartOffset, long remoteEndOffset) {
        return new DisklessSwitchReplicaDescription(offline, logStartOffset, logEndOffset, highWatermark,
            crossTierLogStartOffset, remoteStartOffset, remoteEndOffset);
    }

    private void mockPhaseOnePartition() {
        mockDescribe(List.of(buildPartition(0, 1, 7, SEAL, 7)
            .setReplicaNodes(List.of(1, 2, 3))
            .setIsrNodes(List.of(1))));
    }

    private void mockReplica(int brokerId, DisklessSwitchReplicaDescription description) {
        DescribeDisklessSwitchReplicaResult result = mock(DescribeDisklessSwitchReplicaResult.class);
        when(result.description()).thenReturn(KafkaFuture.completedFuture(description));
        when(adminClient.describeDisklessSwitchReplica(TOPIC, 0, brokerId)).thenReturn(result);
    }

    private void mockReplicaError(int brokerId, Exception error) {
        KafkaFutureImpl<DisklessSwitchReplicaDescription> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        DescribeDisklessSwitchReplicaResult result = mock(DescribeDisklessSwitchReplicaResult.class);
        when(result.description()).thenReturn(future);
        when(adminClient.describeDisklessSwitchReplica(TOPIC, 0, brokerId)).thenReturn(result);
    }

    private String runDescribeCommand() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, false, StandardCharsets.UTF_8);
        TopicSwitchCommand.describeCommand(stream, adminClient, TOPIC, 0);
        return out.toString(StandardCharsets.UTF_8);
    }

    private String runElectCommand(int replica, boolean dryRun) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, false, StandardCharsets.UTF_8);
        TopicSwitchCommand.electCommand(stream, adminClient, TOPIC, 0, replica, dryRun);
        return out.toString(StandardCharsets.UTF_8);
    }

    private String runStartPhaseTwoCommand(boolean acceptDataLoss, boolean dryRun) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, false, StandardCharsets.UTF_8);
        TopicSwitchCommand.startPhaseTwoCommand(stream, adminClient, TOPIC, 0, acceptDataLoss, dryRun);
        return out.toString(StandardCharsets.UTF_8);
    }

    private void mockLogRange(int partition, long startOffset, long endOffset) {
        when(adminClient.listOffsets(anyMap()))
            .thenAnswer(invocation -> {
                Map<TopicPartition, OffsetSpec> request = invocation.getArgument(0);
                boolean isLatest = request.values().stream().anyMatch(s -> s instanceof OffsetSpec.LatestSpec);
                long offset = isLatest ? endOffset : startOffset;
                TopicPartition tp = new TopicPartition(TOPIC, partition);
                Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = Map.of(
                    tp, KafkaFuture.completedFuture(
                        new ListOffsetsResult.ListOffsetsResultInfo(offset, -1, Optional.empty())));
                return new ListOffsetsResult(futures);
            });
    }

    // Mocks describeTopicPartitions so the seal command can read the current committed offset.
    private void mockDescribeSeal(int partition, long classicToDisklessStartOffset) {
        mockDescribeSeal(partition, classicToDisklessStartOffset, false);
    }

    private void mockDescribeSeal(int partition, long classicToDisklessStartOffset, boolean classicPrefixInRemoteStorage) {
        DescribeTopicPartitionsResponseData responseData = buildResponseData(List.of(
            buildPartition(partition, 1, 0, classicToDisklessStartOffset, -1, classicPrefixInRemoteStorage)));
        when(adminClient.describeTopicPartitions(ArgumentMatchers.any(), any(DescribeTopicsOptions.class)))
            .thenReturn(new DescribeTopicPartitionsResult(KafkaFuture.completedFuture(responseData)));
    }

    private void mockAlterDisklessSwitch() {
        AlterDisklessSwitchResult result = mock(AlterDisklessSwitchResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.alterDisklessSwitch(any(), anyInt(), anyLong(), any(AlterDisklessSwitchOptions.class)))
            .thenReturn(result);
    }

    private void mockDescribe(List<DescribeTopicPartitionsResponsePartition> partitions) {
        DescribeTopicPartitionsResponseData responseData = buildResponseData(partitions);
        when(adminClient.describeTopicPartitions(ArgumentMatchers.any(), any(DescribeTopicsOptions.class)))
            .thenReturn(new DescribeTopicPartitionsResult(KafkaFuture.completedFuture(responseData)));
    }

    private void mockRepairDisklessLog() {
        RepairDisklessLogResult result = mock(RepairDisklessLogResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.repairDisklessLog(any(), anyInt(), anyInt())).thenReturn(result);
    }

    private String runSealCommand(int partition, Optional<Long> offset, boolean dryRun) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, false, StandardCharsets.UTF_8);
        TopicSwitchCommand.sealCommand(stream, adminClient, TOPIC, partition, offset, false, dryRun);
        return out.toString(StandardCharsets.UTF_8);
    }

    private String runRepairCommand(Optional<Integer> partition) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, false, StandardCharsets.UTF_8);
        TopicSwitchCommand.repairCommand(stream, adminClient, TOPIC, partition);
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
