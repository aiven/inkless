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
import org.apache.kafka.clients.admin.RearmDisklessSwitchOptions;
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
            case "rearm":
                try (Admin adminClient = Admin.create(properties)) {
                    rearmCommand(System.out, adminClient, topic, namespace.getInt("partition"));
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
        Subparser rearmParser = commandParsers.addParser("rearm")
                .help("Re-arm the pending switch of a partition, so its leader seals again and re-runs the switch.");

        for (Subparser subparser : List.of(stateParser, rearmParser)) {
            MutuallyExclusiveGroup connectionOptions = subparser.addMutuallyExclusiveGroup().required(true);
            connectionOptions.addArgument("--bootstrap-server", "-b")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the Kafka cluster.");
            connectionOptions.addArgument("--bootstrap-controller", "-C")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the KRaft controllers.");
            subparser.addArgument("--command-config", "-c")
                    .action(store())
                    .help("Config properties file for the Admin client.");
            subparser.addArgument("--topic", "-t")
                    .action(store())
                    .required(true)
                    .help("Topic name for the specified topic.");
        }

        rearmParser.addArgument("--partition", "-p")
                .action(store())
                .type(Integer.class)
                .required(true)
                .help("The partition index to re-arm. The controller accepts the request only while the "
                        + "switch of the partition is pending (classicToDisklessStartOffset=-2).");

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
            boolean classicPrefixInRemoteStorage = InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(
                partition.unknownTaggedFields());
            int lastClassicLeaderEpoch = InitDisklessLogFields.decodeLastClassicLeaderEpoch(
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
            stream.printf("  classicPrefixInRemoteStorage=%s%n",
                formatClassicPrefixInRemoteStorage(classicPrefixInRemoteStorage, classicToDisklessStartOffset));
            stream.printf("  lastClassicLeaderEpoch=%s%n",
                formatLastClassicLeaderEpoch(lastClassicLeaderEpoch));
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

    static void rearmCommand(PrintStream stream, Admin adminClient, String topic, int partition) throws Exception {
        adminClient.rearmDisklessSwitch(topic, partition, new RearmDisklessSwitchOptions()).all().get();
        stream.printf("Re-armed the switch of %s-%d.%n", topic, partition);
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

    private static String formatLastClassicLeaderEpoch(int epoch) {
        if (epoch == PartitionRegistration.NO_LAST_CLASSIC_LEADER_EPOCH) {
            return "-1 (none)";
        } else {
            return String.valueOf(epoch);
        }
    }

    private static String formatClassicPrefixInRemoteStorage(boolean flag, long classicToDisklessStartOffset) {
        if (classicToDisklessStartOffset < 0) {
            return String.valueOf(flag);
        }
        return flag ? "true (phase 2)" : "false (phase 1)";
    }
}
