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
import org.apache.kafka.clients.admin.DisklessSwitchReplicaDescription;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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
            case "describe":
                try (Admin adminClient = Admin.create(properties)) {
                    describeCommand(System.out, adminClient, topic, namespace.getInt("partition"));
                }
                break;
            case "elect":
                try (Admin adminClient = Admin.create(properties)) {
                    electCommand(System.out, adminClient, topic,
                        namespace.getInt("partition"),
                        namespace.getInt("replica"),
                        namespace.getBoolean("dry_run"));
                }
                break;
            case "start-phase-2":
                try (Admin adminClient = Admin.create(properties)) {
                    startPhaseTwoCommand(System.out, adminClient, topic,
                        namespace.getInt("partition"),
                        namespace.getBoolean("accept_data_loss"),
                        namespace.getBoolean("dry_run"));
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
        Subparser describeParser = commandParsers.addParser("describe")
                .help("Describe each replica of a partition in phase 1 of the switch, and how much of the "
                        + "classic prefix it and remote storage hold.");
        Subparser electParser = commandParsers.addParser("elect")
                .help("Elect a replica that holds the whole classic prefix as the leader of a phase 1 partition, "
                        + "even if it's outside the ISR. Use --dry-run to perform validation only.");
        Subparser startPhaseTwoParser = commandParsers.addParser("start-phase-2")
                .help("Start phase 2 of a phase 1 partition at the end of its classic prefix in remote storage. "
                        + "The records from there to the seal are lost. Use --dry-run to perform validation only.");
        List<Subparser> recoveryParsers = List.of(describeParser, electParser, startPhaseTwoParser);

        for (Subparser subparser : List.of(stateParser, sealParser)) {
            MutuallyExclusiveGroup connectionOptions = subparser.addMutuallyExclusiveGroup().required(true);
            connectionOptions.addArgument("--bootstrap-server", "-b")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the Kafka cluster.");
            connectionOptions.addArgument("--bootstrap-controller", "-C")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the KRaft controllers.");
        }
        for (Subparser subparser : List.of(repairParser, describeParser, electParser, startPhaseTwoParser)) {
            subparser.addArgument("--bootstrap-server", "-b")
                    .action(store())
                    .required(true)
                    .help("A list of host/port pairs to use for establishing the connection to the Kafka cluster.");
        }

        for (Subparser subparser : List.of(stateParser, sealParser, repairParser, describeParser, electParser,
                startPhaseTwoParser)) {
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
                        + "Once a seal is committed, a new seal offset can't exceed it. "
                        + "Once the partition's classic prefix is in remote storage (classicPrefixInRemoteStorage=true "
                        + "in the state command), the controller rejects every seal offset. "
                        + "If omitted, the partition's current end offset is used; omitting it is only allowed "
                        + "before a seal is committed, otherwise an explicit offset is required.");
        sealParser.addArgument("--clear-producer-states")
                .action(storeTrue())
                .help("When forcing a seal (offset >= 0), clear the committed producer states. Ignored for "
                        + "negative offsets, which always clear them.");
        sealParser.addArgument("--dry-run", "-d")
                .action(storeTrue())
                .help("Whether to only perform validation when adjusting the seal offset. The validation runs "
                        + "in the command and doesn't cover -1 and -2, so the controller can still reject the request.");

        repairParser.addArgument("--partition", "-p")
                .action(store())
                .type(Integer.class)
                .help("The partition index to repair. If omitted, all switched partitions of the topic are repaired.");

        for (Subparser subparser : recoveryParsers) {
            subparser.addArgument("--partition", "-p")
                    .action(store())
                    .type(Integer.class)
                    .required(true)
                    .help("The partition index.");
        }
        electParser.addArgument("--replica", "-r")
                .action(store())
                .type(Integer.class)
                .required(true)
                .help("The broker ID of the replica to elect. The command checks that it holds the whole "
                        + "classic prefix, together with remote storage.");
        startPhaseTwoParser.addArgument("--accept-data-loss")
                .action(storeTrue())
                .help("Start phase 2 even if remote storage doesn't hold the whole classic prefix. The records "
                        + "that only the local disks held are lost.");
        for (Subparser subparser : List.of(electParser, startPhaseTwoParser)) {
            subparser.addArgument("--dry-run", "-d")
                    .action(storeTrue())
                    .help("Whether to only perform validation. The controller can still reject the request.");
        }

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
            SwitchState state = readSwitchState(adminClient, topic, partition);
            rejectIfClassicPrefixInRemoteStorage(state, topic, partition);
            if (state.classicToDisklessStartOffset() >= 0) {
                throw new RuntimeException(String.format(
                    "%s-%d already has a committed seal offset (%d); pass an explicit --offset to re-seal.",
                    topic, partition, state.classicToDisklessStartOffset()));
            }
            sealOffset = endOffset(adminClient, topic, partition);
            stream.printf("Validated %s-%d: sealing at end offset %d.%n", topic, partition, sealOffset);
        } else {
            sealOffset = offset.get();
            if (sealOffset >= 0) {
                SwitchState state = readSwitchState(adminClient, topic, partition);
                rejectIfClassicPrefixInRemoteStorage(state, topic, partition);
                long committedSeal = state.classicToDisklessStartOffset();
                if (committedSeal >= 0 && sealOffset > committedSeal) {
                    throw new RuntimeException(String.format(
                        "Cannot seal %s-%d at offset %d: it exceeds the committed seal offset (%d).",
                        topic, partition, sealOffset, committedSeal));
                }
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

    static void describeCommand(PrintStream stream, Admin adminClient, String topic, int partition) throws Exception {
        RecoveryView view = readRecoveryView(adminClient, topic, partition);
        DescribeTopicPartitionsResponsePartition metadata = view.partition();
        stream.printf("Partition %s-%d:%n", topic, partition);
        stream.printf("  leader=%d epoch=%d isr=%s replicas=%s%n",
            metadata.leaderId(), metadata.leaderEpoch(), metadata.isrNodes(), metadata.replicaNodes());
        stream.printf("  classicToDisklessStartOffset=%s classicPrefixInRemoteStorage=%s%n",
            formatStartOffset(view.seal()),
            formatClassicPrefixInRemoteStorage(view.classicPrefixInRemoteStorage(), view.seal()));
        stream.printf("  crossTierLogStartOffset=%d remoteStartOffset=%d remoteEndOffset=%d%n",
            view.crossTierLogStart(), view.remoteStart(), view.remoteEnd());
        for (ReplicaView replica : view.replicas()) {
            if (replica.description().isEmpty()) {
                stream.printf("  Broker %d: %s%n", replica.brokerId(), replica.error());
                continue;
            }
            DisklessSwitchReplicaDescription description = replica.description().get();
            stream.printf("  Broker %d: offline=%s logStartOffset=%d logEndOffset=%d highWatermark=%d%n",
                replica.brokerId(), description.offline(), description.logStartOffset(),
                description.logEndOffset(), description.highWatermark());
            if (view.isPhaseOne()) {
                stream.printf("    %s%n", view.missingClassicPrefix(description)
                    .map(reason -> "Misses part of the classic prefix: " + reason + ".")
                    .orElse("Holds the whole classic prefix, together with remote storage."));
            }
        }
        if (!view.isPhaseOne()) {
            stream.println("  The partition isn't in phase 1, so it doesn't need recovery.");
            return;
        }
        if (view.remoteEnd() >= 0) {
            stream.printf("  start-phase-2 would set the seal to %d.%s%n",
                view.remoteEnd(), view.lostRanges().map(lost -> " It would lose " + lost + ".").orElse(""));
        }
    }

    static void electCommand(PrintStream stream, Admin adminClient, String topic, int partition, int replica,
                             boolean dryRun) throws Exception {
        RecoveryView view = readRecoveryView(adminClient, topic, partition);
        view.requirePhaseOne(topic, partition);
        if (!view.partition().replicaNodes().contains(replica)) {
            throw new RuntimeException(String.format("Broker %d isn't a replica of %s-%d (replicas=%s).",
                replica, topic, partition, view.partition().replicaNodes()));
        }
        if (view.partition().leaderId() == replica) {
            throw new RuntimeException(String.format("Broker %d is already the leader of %s-%d.",
                replica, topic, partition));
        }
        ReplicaView replicaView = view.replica(replica);
        DisklessSwitchReplicaDescription description = replicaView.description().orElseThrow(() ->
            new RuntimeException(String.format("Cannot describe broker %d's replica of %s-%d: %s",
                replica, topic, partition, replicaView.error())));
        Optional<String> missing = view.missingClassicPrefix(description);
        if (missing.isPresent()) {
            throw new RuntimeException(String.format(
                "Cannot elect broker %d for %s-%d: it misses part of the classic prefix below the seal %d: %s.",
                replica, topic, partition, view.seal(), missing.get()));
        }
        stream.printf("Validated %s-%d: broker %d holds the classic prefix up to the seal %d "
                + "(logStartOffset=%d highWatermark=%d), together with remote storage.%n",
            topic, partition, replica, view.seal(), description.logStartOffset(), description.highWatermark());

        if (dryRun) {
            stream.printf("[dry-run] Would elect broker %d as the leader of %s-%d.%n", replica, topic, partition);
            return;
        }
        adminClient.alterDisklessSwitch(topic, partition, view.seal(),
            new AlterDisklessSwitchOptions().leaderId(replica)).all().get();
        stream.printf("Elected broker %d as the leader of %s-%d. The other replicas rejoin the ISR once they "
            + "catch up.%n", replica, topic, partition);
    }

    static void startPhaseTwoCommand(PrintStream stream, Admin adminClient, String topic, int partition,
                                     boolean acceptDataLoss, boolean dryRun) throws Exception {
        RecoveryView view = readRecoveryView(adminClient, topic, partition);
        view.requirePhaseOne(topic, partition);
        for (ReplicaView replica : view.replicas()) {
            if (replica.description().isPresent()
                    && view.missingClassicPrefix(replica.description().get()).isEmpty()) {
                if (replica.brokerId() == view.partition().leaderId()) {
                    throw new RuntimeException(String.format(
                        "The leader of %s-%d (broker %d) holds the whole classic prefix, so the partition doesn't "
                            + "need recovery. The leader starts phase 2 once remote storage holds the prefix.",
                        topic, partition, replica.brokerId()));
                }
                throw new RuntimeException(String.format(
                    "Broker %d holds the whole classic prefix of %s-%d. Elect it with the elect command instead, "
                        + "which loses no records.", replica.brokerId(), topic, partition));
            }
        }
        if (view.remoteEnd() < 0) {
            throw new RuntimeException(String.format(
                "No replica of %s-%d reports how much of the classic prefix remote storage holds. Retry once "
                    + "the remote log manager is ready.", topic, partition));
        }
        for (ReplicaView replica : view.replicas()) {
            if (replica.description().isEmpty()) {
                stream.printf("Warning: broker %d's replica isn't checked: %s%n", replica.brokerId(), replica.error());
            }
        }
        long newSeal = view.remoteEnd();
        Optional<String> lostRanges = view.lostRanges();
        if (lostRanges.isPresent() && !acceptDataLoss) {
            throw new RuntimeException(String.format(
                "Starting phase 2 for %s-%d at seal %d loses the records in %s, which no replica or remote "
                    + "storage holds. Pass --accept-data-loss to start phase 2 anyway.",
                topic, partition, newSeal, lostRanges.get()));
        }
        stream.printf("Validated %s-%d: remote storage holds the classic prefix up to %d, and the committed seal "
            + "is %d.%n", topic, partition, newSeal, view.seal());
        lostRanges.ifPresent(lost -> stream.printf("Accepting the loss of the records in %s.%n", lost));

        if (dryRun) {
            stream.printf("[dry-run] Would start phase 2 for %s-%d at seal %d.%n", topic, partition, newSeal);
            return;
        }
        adminClient.alterDisklessSwitch(topic, partition, newSeal,
            new AlterDisklessSwitchOptions().classicPrefixInRemoteStorage(true)).all().get();
        stream.printf("Started phase 2 for %s-%d at seal %d.%n", topic, partition, newSeal);
        if (newSeal != view.seal()) {
            stream.printf("Run the repair command for %s-%d to set the control-plane diskless log start to %d.%n",
                topic, partition, newSeal);
        }
    }

    private record ReplicaView(int brokerId, Optional<DisklessSwitchReplicaDescription> description, String error) { }

    /**
     * Holds a switching partition, its replicas, and the run of remote segments without a gap that the
     * replica with the highest remote end reports. Brokers read the remote log metadata at different
     * speeds, so that replica has the most current view.
     */
    private record RecoveryView(
        DescribeTopicPartitionsResponsePartition partition,
        long seal,
        boolean classicPrefixInRemoteStorage,
        List<ReplicaView> replicas,
        long crossTierLogStart,
        long remoteStart,
        long remoteEnd
    ) {
        boolean isPhaseOne() {
            return seal >= 0 && !classicPrefixInRemoteStorage;
        }

        void requirePhaseOne(String topic, int partitionIndex) {
            if (!isPhaseOne()) {
                throw new RuntimeException(String.format(
                    "%s-%d isn't in phase 1 of the switch (classicToDisklessStartOffset=%s, "
                        + "classicPrefixInRemoteStorage=%s).",
                    topic, partitionIndex, formatStartOffset(seal), classicPrefixInRemoteStorage));
            }
        }

        ReplicaView replica(int brokerId) {
            return replicas.stream().filter(replica -> replica.brokerId() == brokerId).findFirst().orElseThrow();
        }

        /**
         * Returns why the replica and remote storage together don't hold every offset from the cross-tier
         * log start to the seal, or empty if they do. Only records below the high watermark count, because
         * the replica may not have committed the records above it.
         */
        Optional<String> missingClassicPrefix(DisklessSwitchReplicaDescription description) {
            if (description.offline()) {
                return Optional.of("the broker took the partition offline because its log ends below the seal");
            }
            if (description.logEndOffset() < 0) {
                return Optional.of("it has no local log");
            }
            if (description.highWatermark() < seal) {
                return Optional.of("its high watermark " + description.highWatermark() + " is below the seal");
            }
            long logStart = description.logStartOffset();
            if (logStart == 0 || (crossTierLogStart >= 0 && logStart <= crossTierLogStart)) {
                return Optional.empty();
            }
            if (remoteEnd < 0) {
                return Optional.of("its log starts at " + logStart + ", and no replica reports the remote copy "
                    + "of the prefix");
            }
            if (remoteStart > crossTierLogStart) {
                return Optional.of("its log starts at " + logStart + ", and remote storage misses the offsets "
                    + "from " + crossTierLogStart + " to " + remoteStart);
            }
            if (logStart > remoteEnd) {
                return Optional.of("its log starts at " + logStart + ", and remote storage holds the prefix "
                    + "only up to " + remoteEnd);
            }
            return Optional.empty();
        }

        /**
         * Returns the offset ranges that starting phase 2 at {@link #remoteEnd()} loses, or empty if it
         * loses none.
         */
        Optional<String> lostRanges() {
            List<String> lost = new ArrayList<>();
            if (remoteStart > crossTierLogStart) {
                lost.add("[" + crossTierLogStart + ", " + remoteStart + ")");
            }
            if (remoteEnd < seal) {
                lost.add("[" + remoteEnd + ", " + seal + ")");
            }
            return lost.isEmpty() ? Optional.empty() : Optional.of(String.join(" and ", lost));
        }
    }

    private static RecoveryView readRecoveryView(Admin adminClient, String topic, int partition) throws Exception {
        DescribeTopicPartitionsResponsePartition metadata = describePartition(adminClient, topic, partition);
        List<ReplicaView> replicas = new ArrayList<>();
        for (int brokerId : metadata.replicaNodes()) {
            try {
                DisklessSwitchReplicaDescription description = adminClient
                    .describeDisklessSwitchReplica(topic, partition, brokerId).description().get();
                replicas.add(new ReplicaView(brokerId, Optional.of(description), null));
            } catch (ExecutionException e) {
                replicas.add(new ReplicaView(brokerId, Optional.empty(), e.getCause().toString()));
            }
        }
        List<DisklessSwitchReplicaDescription> descriptions = replicas.stream()
            .flatMap(replica -> replica.description().stream())
            .toList();
        Optional<DisklessSwitchReplicaDescription> latestRemote = descriptions.stream()
            .filter(description -> description.remoteEndOffset() >= 0)
            .max(Comparator.comparingLong(DisklessSwitchReplicaDescription::remoteEndOffset));
        long crossTierLogStart = latestRemote
            .map(DisklessSwitchReplicaDescription::crossTierLogStartOffset)
            .orElseGet(() -> descriptions.stream()
                .mapToLong(DisklessSwitchReplicaDescription::crossTierLogStartOffset)
                .max()
                .orElse(-1L));
        return new RecoveryView(
            metadata,
            InitDisklessLogFields.decodeClassicToDisklessStartOffset(metadata.unknownTaggedFields()),
            InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(metadata.unknownTaggedFields()),
            replicas,
            crossTierLogStart,
            latestRemote.map(DisklessSwitchReplicaDescription::remoteStartOffset).orElse(-1L),
            latestRemote.map(DisklessSwitchReplicaDescription::remoteEndOffset).orElse(-1L));
    }

    private static long startOffset(Admin adminClient, String topic, int partition) throws Exception {
        TopicPartition tp = new TopicPartition(topic, partition);
        return adminClient.listOffsets(Map.of(tp, OffsetSpec.earliest())).all().get().get(tp).offset();
    }

    private static long endOffset(Admin adminClient, String topic, int partition) throws Exception {
        TopicPartition tp = new TopicPartition(topic, partition);
        return adminClient.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get().get(tp).offset();
    }

    private record SwitchState(long classicToDisklessStartOffset, boolean classicPrefixInRemoteStorage) { }

    private static SwitchState readSwitchState(Admin adminClient, String topic, int partition) throws Exception {
        DescribeTopicPartitionsResponsePartition p = describePartition(adminClient, topic, partition);
        return new SwitchState(
            InitDisklessLogFields.decodeClassicToDisklessStartOffset(p.unknownTaggedFields()),
            InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(p.unknownTaggedFields()));
    }

    private static DescribeTopicPartitionsResponsePartition describePartition(Admin adminClient, String topic,
                                                                              int partition) throws Exception {
        DescribeTopicPartitionsResponseData responseData = adminClient
            .describeTopicPartitions(List.of(topic), new DescribeTopicsOptions()).rawResponse().get();
        DescribeTopicPartitionsResponseTopic topicData = responseData.topics().find(topic);
        if (topicData == null) {
            throw new RuntimeException("Topic not found: " + topic);
        }
        return topicData.partitions().stream()
            .filter(p -> p.partitionIndex() == partition)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Partition not found: " + topic + "-" + partition));
    }

    // The controller rejects the request too. Failing here keeps --dry-run from reporting an action that
    // can't succeed.
    private static void rejectIfClassicPrefixInRemoteStorage(SwitchState state, String topic, int partition) {
        if (state.classicPrefixInRemoteStorage()) {
            throw new RuntimeException(String.format(
                "%s-%d has its classic prefix in remote storage (classicPrefixInRemoteStorage=true); "
                    + "the controller rejects every seal change in that state.",
                topic, partition));
        }
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

    private static String formatClassicPrefixInRemoteStorage(boolean flag, long classicToDisklessStartOffset) {
        if (classicToDisklessStartOffset < 0) {
            return String.valueOf(flag);
        }
        return flag ? "true (phase 2)" : "false (phase 1)";
    }
}
