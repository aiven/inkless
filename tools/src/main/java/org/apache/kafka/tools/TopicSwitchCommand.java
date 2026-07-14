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
import org.apache.kafka.clients.admin.DescribeTopicPartitionsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.InitDisklessLogFields;
import org.apache.kafka.metadata.InitDisklessLogFields.ProducerStateEntry;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.util.CommandLineUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public abstract class TopicSwitchCommand {
    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    private static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ArgumentParser parser = argumentParser();
        Namespace namespace = parser.parseArgsOrFail(args);
        String command = namespace.getString("command");
        String commandConfigFile = namespace.getString("command_config");
        String topic =  namespace.getString("topic");

        Properties properties = (commandConfigFile != null) ? Utils.loadProps(commandConfigFile) : new Properties();
        CommandLineUtils.initializeBootstrapProperties(properties,
                Optional.ofNullable(namespace.getString("bootstrap_server")),
                Optional.ofNullable(namespace.getString("bootstrap_controller")));

        switch (command) {
            case "state":
                try (Admin adminClient = Admin.create(properties)) {
                    stateCommand(System.out, adminClient, topic);
                }
                break;
            case "seal":
                try (Admin adminClient = Admin.create(properties)) {
                    sealCommand(System.out, adminClient, topic,
                        namespace.getInt("partition"),
                        Optional.ofNullable(namespace.getLong("offset")),
                        namespace.getBoolean("clear_producer_states"),
                        namespace.getBoolean("dry_run"));
                }
                break;
            case "repair":
                try (Admin adminClient = Admin.create(properties)) {
                    repairCommand(System.out, adminClient, topic,
                        Optional.ofNullable(namespace.getInt("partition")));
                }
                break;
            default:
                throw new RuntimeException("Unknown command " + command);
        }
    }

    static ArgumentParser argumentParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-topic-switch")
                .defaultHelp(true)
                .description("The Kafka topic switch tool.");
        Subparsers commandParsers = parser.addSubparsers().dest("command");

        Subparser stateParser = commandParsers.addParser("state")
                .help("Print the switch state for each partition of a specified topic.");
        Subparser sealParser = commandParsers.addParser("seal")
                .help("Override the topic seal offset. Use --dry-run to perform validation only.");
        Subparser repairParser = commandParsers.addParser("repair")
                .help("Repair the control-plane diskless log entry.");

        for (Subparser subparser : List.of(stateParser, sealParser)) {
            MutuallyExclusiveGroup connectionOptions = subparser.addMutuallyExclusiveGroup().required(true);
            connectionOptions.addArgument("--bootstrap-server", "-b")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the Kafka cluster.");
            connectionOptions.addArgument("--bootstrap-controller", "-C")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the KRaft controllers.");
        }
        repairParser.addArgument("--bootstrap-server", "-b")
                .action(store())
                .required(true)
                .help("A list of host/port pairs to use for establishing the connection to the Kafka cluster.");

        for (Subparser subparser : List.of(stateParser, sealParser, repairParser)) {
            subparser.addArgument("--command-config", "-c")
                    .action(store())
                    .help("Config properties file for the Admin client.");
            subparser.addArgument("--topic", "-t")
                    .action(store())
                    .required(true)
                    .help("Topic name for the specified topic.");
        }

        sealParser.addArgument("--partition", "-p")
                .action(store())
                .type(Integer.class)
                .required(true)
                .help("The partition index to seal.");
        sealParser.addArgument("--offset", "-o")
                .action(store())
                .type(Long.class)
                .help("The seal offset to commit: >= 0 forces (re-)sealing at that offset, -1 aborts the "
                        + "switch and reverts the partition to classic, and -2 re-arms the switch as pending. "
                        + "-1 and -2 are only allowed while the switch is still pending (no seal committed yet). "
                        + "If omitted, the partition's current end offset is used; omitting it is only allowed "
                        + "before a seal is committed, otherwise an explicit offset is required.");
        sealParser.addArgument("--clear-producer-states")
                .action(storeTrue())
                .help("When forcing a seal (offset >= 0), clear the committed producer states. Ignored for "
                        + "negative offsets, which always clear them.");
        sealParser.addArgument("--dry-run", "-d")
                .action(storeTrue())
                .help("Whether to only perform validation when adjusting the seal offset.");

        repairParser.addArgument("--partition", "-p")
                .action(store())
                .type(Integer.class)
                .help("The partition index to repair. If omitted, all switched partitions of the topic are repaired.");

        return parser;
    }

    static void stateCommand(PrintStream stream, Admin adminClient, String topic) throws Exception {
        DescribeTopicPartitionsResult describeResult = adminClient.describeTopicPartitions(
            List.of(topic), new DescribeTopicsOptions());
        DescribeTopicPartitionsResponseData responseData = describeResult.rawResponse().get();

        DescribeTopicPartitionsResponseTopic topicData = responseData.topics().find(topic);
        if (topicData == null) {
            throw new RuntimeException("Topic not found: " + topic);
        }
        if (topicData.errorCode() != 0) {
            throw new RuntimeException("Error describing topic " + topic + ": error code " + topicData.errorCode());
        }

        List<DescribeTopicPartitionsResponsePartition> partitions = topicData.partitions();

        Map<TopicPartition, OffsetSpec> latestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> earliestOffsets = new HashMap<>();
        for (DescribeTopicPartitionsResponsePartition partition : partitions) {
            TopicPartition tp = new TopicPartition(topic, partition.partitionIndex());
            latestOffsets.put(tp, OffsetSpec.latest());
            earliestOffsets.put(tp, OffsetSpec.earliest());
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
            adminClient.listOffsets(latestOffsets).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
            adminClient.listOffsets(earliestOffsets).all().get();

        for (DescribeTopicPartitionsResponsePartition partition : partitions) {
            int partitionIndex = partition.partitionIndex();
            TopicPartition tp = new TopicPartition(topic, partitionIndex);

            long classicToDisklessStartOffset = InitDisklessLogFields.decodeClassicToDisklessStartOffset(
                partition.unknownTaggedFields());
            int disklessLeaderEpoch = InitDisklessLogFields.decodeDisklessLeaderEpoch(
                partition.unknownTaggedFields());
            List<ProducerStateEntry> producerStates = InitDisklessLogFields.decodeProducerStates(
                partition.unknownTaggedFields());

            ListOffsetsResult.ListOffsetsResultInfo latestInfo = latest.get(tp);
            ListOffsetsResult.ListOffsetsResultInfo earliestInfo = earliest.get(tp);

            stream.printf("Partition %d:%n", partitionIndex);
            stream.printf("  leader=%d epoch=%d isr=%s%n",
                partition.leaderId(), partition.leaderEpoch(), partition.isrNodes());
            stream.printf("  classicToDisklessStartOffset=%s%n",
                formatStartOffset(classicToDisklessStartOffset));
            stream.printf("  disklessLeaderEpoch=%s%n",
                formatDisklessLeaderEpoch(disklessLeaderEpoch));
            stream.printf("  logStartOffset=%d logEndOffset=%d%n",
                earliestInfo != null ? earliestInfo.offset() : -1,
                latestInfo != null ? latestInfo.offset() : -1);

            if (!producerStates.isEmpty()) {
                stream.printf("  producerStates (%d):%n", producerStates.size());
                for (ProducerStateEntry entry : producerStates) {
                    stream.printf("    producerId=%d epoch=%d seq=%d..%d offset=%d timestamp=%d%n",
                        entry.producerId(), entry.producerEpoch(),
                        entry.baseSequence(), entry.lastSequence(),
                        entry.assignedOffset(), entry.batchMaxTimestamp());
                }
            }
            stream.println();
        }
    }

    static void sealCommand(PrintStream stream, Admin adminClient, String topic, int partition,
                            Optional<Long> offset, boolean clearProducerStates, boolean dryRun) throws Exception {
        if (offset.isPresent() && offset.get() < PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING) {
            throw new RuntimeException("Invalid seal offset " + offset.get()
                + "; must be >= -2 (-2 re-arms, -1 aborts, >= 0 seals at that offset).");
        }

        long sealOffset;
        if (offset.isEmpty()) {
            long committedSeal = readClassicToDisklessStartOffset(adminClient, topic, partition);
            if (committedSeal >= 0) {
                throw new RuntimeException(String.format(
                    "%s-%d already has a committed seal offset (%d); pass an explicit --offset to re-seal.",
                    topic, partition, committedSeal));
            }
            sealOffset = endOffset(adminClient, topic, partition);
            stream.printf("Validated %s-%d: sealing at end offset %d.%n", topic, partition, sealOffset);
        } else {
            sealOffset = offset.get();
            if (sealOffset >= 0) {
                long startOffset = startOffset(adminClient, topic, partition);
                long endOffset = endOffset(adminClient, topic, partition);
                if (sealOffset < startOffset || sealOffset > endOffset) {
                    throw new RuntimeException(String.format(
                        "Cannot seal %s-%d at offset %d: it is outside the classic log range [%d, %d].",
                        topic, partition, sealOffset, startOffset, endOffset));
                }
                stream.printf("Validated %s-%d: seal offset %d within classic log range [%d, %d].%n",
                    topic, partition, sealOffset, startOffset, endOffset);
            }
        }

        if (dryRun) {
            stream.printf("[dry-run] Would set %s-%d classicToDisklessStartOffset to %s.%n",
                topic, partition, describeSealOffset(sealOffset));
            return;
        }

        AlterDisklessSwitchOptions options = new AlterDisklessSwitchOptions()
            .clearProducerStates(clearProducerStates);
        adminClient.alterDisklessSwitch(topic, partition, sealOffset, options).all().get();
        stream.printf("Set %s-%d classicToDisklessStartOffset to %s.%n",
            topic, partition, describeSealOffset(sealOffset));
    }

    static void repairCommand(PrintStream stream, Admin adminClient, String topic,
                              Optional<Integer> partition) throws Exception {
        DescribeTopicPartitionsResponseData responseData = adminClient
            .describeTopicPartitions(List.of(topic), new DescribeTopicsOptions()).rawResponse().get();
        DescribeTopicPartitionsResponseTopic topicData = responseData.topics().find(topic);
        if (topicData == null) {
            throw new RuntimeException("Topic not found: " + topic);
        }
        if (topicData.errorCode() != 0) {
            throw new RuntimeException("Error describing topic " + topic + ": error code " + topicData.errorCode());
        }

        List<DescribeTopicPartitionsResponsePartition> partitions = topicData.partitions().stream()
            .filter(p -> partition.isEmpty() || p.partitionIndex() == partition.get())
            .toList();
        if (partition.isPresent() && partitions.isEmpty()) {
            throw new RuntimeException("Partition not found: " + topic + "-" + partition.get());
        }

        int repaired = 0;
        for (DescribeTopicPartitionsResponsePartition p : partitions) {
            long seal = InitDisklessLogFields.decodeClassicToDisklessStartOffset(p.unknownTaggedFields());
            // Only committed partitions have a control-plane entry to reconcile.
            if (seal < 0) {
                stream.printf("Skipping %s-%d: not switched (classicToDisklessStartOffset=%s).%n",
                    topic, p.partitionIndex(), formatStartOffset(seal));
                continue;
            }
            if (p.leaderId() < 0) {
                stream.printf("Skipping %s-%d: no current leader (leaderId=%d).%n",
                    topic, p.partitionIndex(), p.leaderId());
                continue;
            }
            // The control-plane write happens on the leader, so route the request there.
            adminClient.repairDisklessLog(topic, p.partitionIndex(), p.leaderId()).all().get();
            stream.printf("Repaired %s-%d control-plane diskless log at seal offset %d (leader %d).%n",
                topic, p.partitionIndex(), seal, p.leaderId());
            repaired++;
        }
        stream.printf("Repaired %d partition(s) of topic %s.%n", repaired, topic);
    }

    private static long startOffset(Admin adminClient, String topic, int partition) throws Exception {
        TopicPartition tp = new TopicPartition(topic, partition);
        return adminClient.listOffsets(Map.of(tp, OffsetSpec.earliest())).all().get().get(tp).offset();
    }

    private static long endOffset(Admin adminClient, String topic, int partition) throws Exception {
        TopicPartition tp = new TopicPartition(topic, partition);
        return adminClient.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get().get(tp).offset();
    }

    private static long readClassicToDisklessStartOffset(Admin adminClient, String topic, int partition)
            throws Exception {
        DescribeTopicPartitionsResponseData responseData = adminClient
            .describeTopicPartitions(List.of(topic), new DescribeTopicsOptions()).rawResponse().get();
        DescribeTopicPartitionsResponseTopic topicData = responseData.topics().find(topic);
        if (topicData == null) {
            throw new RuntimeException("Topic not found: " + topic);
        }
        return topicData.partitions().stream()
            .filter(p -> p.partitionIndex() == partition)
            .findFirst()
            .map(p -> InitDisklessLogFields.decodeClassicToDisklessStartOffset(p.unknownTaggedFields()))
            .orElseThrow(() -> new RuntimeException("Partition not found: " + topic + "-" + partition));
    }

    private static String describeSealOffset(long sealOffset) {
        if (sealOffset == PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET) {
            return "-1 (abort switch, revert to classic)";
        } else if (sealOffset == PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING) {
            return "-2 (re-arm switch)";
        } else {
            return String.valueOf(sealOffset);
        }
    }

    private static String formatStartOffset(long offset) {
        if (offset == PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET) {
            return "-1 (not switched)";
        } else if (offset == PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING) {
            return "-2 (switch pending)";
        } else {
            return String.valueOf(offset);
        }
    }

    private static String formatDisklessLeaderEpoch(int epoch) {
        if (epoch == PartitionRegistration.NO_DISKLESS_LEADER_EPOCH) {
            return "-1 (none)";
        } else {
            return String.valueOf(epoch);
        }
    }
}
