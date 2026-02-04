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

import org.apache.kafka.clients.admin.AddTopicsToMirrorOptions;
import org.apache.kafka.clients.admin.AddTopicsToMirrorResult;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateMirrorOptions;
import org.apache.kafka.clients.admin.CreateMirrorResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeMirrorsOptions;
import org.apache.kafka.clients.admin.DescribeMirrorsResult;
import org.apache.kafka.clients.admin.FindCoordinatorResult;
import org.apache.kafka.clients.admin.ListMirrorsResult;
import org.apache.kafka.clients.admin.MirrorDescription;
import org.apache.kafka.clients.admin.MirrorListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RemoveTopicsFromMirrorOptions;
import org.apache.kafka.clients.admin.RemoveTopicsFromMirrorResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

/**
 * Command-line tool for managing cluster mirrors.
 */
public abstract class MirrorCommand {
    private static final Logger LOG = LoggerFactory.getLogger(MirrorCommand.class);

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
        MirrorCommandOptions opts = new MirrorCommandOptions(args);
        MirrorService mirrorService = new MirrorService(opts.bootstrapServer(), opts.commandConfig(), opts.mirrorConfig());
        int exitCode = 0;
        try {
            if (opts.hasCreateOption()) {
                mirrorService.createMirror(opts);
            } else if (opts.hasAddOption()) {
                mirrorService.addTopicsToMirror(opts);
            } else if (opts.hasRemoveOption()) {
                mirrorService.removeTopicsFromMirror(opts);
            } else if (opts.hasListOption()) {
                mirrorService.listMirrors();
            } else if (opts.hasDescribeOption()) {
                mirrorService.describeMirrors(opts);
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            printException(cause != null ? cause : e);
            exitCode = 1;
        } catch (Throwable e) {
            printException(e);
            exitCode = 1;
        } finally {
            mirrorService.close();
            Exit.exit(exitCode);
        }
    }

    private static void printException(Throwable e) {
        System.out.println("Error while executing mirror command : " + e.getMessage());
        LOG.error(Utils.stackTrace(e));
    }

    /*
     * Service class that handles the core mirror operations using Kafka Admin Client.
     */
    public static class MirrorService implements AutoCloseable {
        private final Admin adminClient;
        private final Properties commandConfig;
        private final Properties mirrorConfigs;

        public MirrorService(Optional<String> bootstrapServer, Properties commandConfig, Properties mirrorConfigs) {
            this.adminClient = createAdminClient(bootstrapServer, commandConfig);
            this.commandConfig = commandConfig;
            this.mirrorConfigs = mirrorConfigs;
        }

        private static Admin createAdminClient(Optional<String> bootstrapServer, Properties commandConfig) {
            bootstrapServer.ifPresent(s -> commandConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s));
            return Admin.create(commandConfig);
        }

        private Node findCoordinator(String mirrorName) throws ExecutionException, InterruptedException {
            FindCoordinatorResult findCoordinatorResult = adminClient.findCoordinator(mirrorName);
            Node coordinator = findCoordinatorResult.node().get();
            if (coordinator == null) {
                throw new RuntimeException("Could not find coordinator for mirror " + mirrorName);
            }
            return coordinator;
        }

        private Set<String> matchTopics(Set<String> allTopics, String topicPattern) {
            Set<String> matchingTopics = new HashSet<>();
            var pattern = java.util.regex.Pattern.compile(topicPattern);
            for (String topic : allTopics) {
                if (topic.equals(topicPattern) || pattern.matcher(topic).matches()) {
                    matchingTopics.add(topic);
                }
            }
            if (matchingTopics.isEmpty()) {
                throw new RuntimeException("No topics matching pattern '" + topicPattern + "' found");
            }
            return matchingTopics;
        }

        public void createMirror(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            Map<String, String> configMap = new HashMap<>();
            mirrorConfigs.forEach((k, v) -> configMap.put(k.toString(), v.toString()));

            CreateMirrorResult result = adminClient.createMirror(
                opts.mirror().get(),
                configMap,
                new CreateMirrorOptions()
            );
            result.all().get();
            System.out.printf("Created mirror %s%n", opts.mirror().get());
        }

        public void addTopicsToMirror(MirrorCommandOptions opts) throws Exception {
            String topicPattern = opts.topic().get();
            String mirrorName = opts.mirror().get();

            // Retrieve the full mirror configuration from the coordinator
            ConfigResource mirrorConfigResource = new ConfigResource(ConfigResource.Type.MIRROR, mirrorName);
            var configResult = adminClient.describeConfigs(List.of(mirrorConfigResource)).all().get();
            var mirrorConfigEntries = configResult.get(mirrorConfigResource);

            if (mirrorConfigEntries == null || mirrorConfigEntries.entries().isEmpty()) {
                throw new RuntimeException("Mirror '" + mirrorName + "' not found or has no configuration");
            }

            // Convert config entries to Properties for creating source Admin client
            Properties sourceConfig = new Properties();
            for (var entry : mirrorConfigEntries.entries()) {
                sourceConfig.put(entry.name(), entry.value());
            }

            // Connect to source cluster and get matching topics
            Set<String> matchingTopics;
            Map<String, String> topicIds = new HashMap<>();
            Map<String, Integer> partNums = new HashMap<>();

            try (Admin sourceAdmin = Admin.create(sourceConfig)) {
                // List all topics from source cluster and match against the pattern
                var allTopics = sourceAdmin.listTopics().names().get();
                matchingTopics = matchTopics(allTopics, topicPattern);

                // Fetch topic IDs for all matching topics
                var topicDescriptions = sourceAdmin.describeTopics(matchingTopics).allTopicNames().get();
                for (var entry : topicDescriptions.entrySet()) {
                    String topic = entry.getKey();
                    TopicDescription desc = entry.getValue();
                    topicIds.put(topic, desc.topicId().toString());
                    partNums.put(topic, desc.partitions().size());
                }
            }

            Node coordinatorNode = findCoordinator(mirrorName);
            String bootstrapServer = coordinatorNode.host() + ":" + coordinatorNode.port();

            try (Admin admin = createAdminClient(Optional.of(bootstrapServer), commandConfig)) {
                // Prepare all NewTopic objects for batch creation
                Set<NewTopic> newTopics = new HashSet<>();
                for (String topicName : matchingTopics) {
                    String topicId = topicIds.get(topicName);
                    int partNum = partNums.get(topicName);
                    NewTopic newTopic = new NewTopic(topicName,
                        Optional.of(partNum), // use source topic partitions
                        opts.replicationFactor(), // use provided replicationFactor or cluster default
                        Optional.of(topicId));
                    newTopics.add(newTopic);
                }

                // Try to create all matching topics
                CreateTopicsResult createResult = admin.createTopics(newTopics,
                    new CreateTopicsOptions().retryOnQuotaViolation(false));

                // Attach created and existing topic to the mirror
                Set<String> createdTopics = new HashSet<>();
                Map<String, String> existingTopics = new HashMap<>();

                for (String topicName : matchingTopics) {
                    try {
                        createResult.values().get(topicName).get();
                        createdTopics.add(topicName);
                    } catch (ExecutionException e) {
                        if (!(e.getCause() instanceof TopicExistsException)) {
                            System.err.printf("Failed to add topic %s: %s%n", topicName, e.getCause().getMessage());
                        }
                    } finally {
                        existingTopics.put(topicName, mirrorName);
                    }
                }

                // TODO: We should return error and let the command retry if the topic metadata is not propagated to brokers. Right now, we sleep 1 sec
                Thread.sleep(1000);
                // Ensures the mirror.name config is properly set even when topics already exist
                AddTopicsToMirrorResult addResult = admin.addTopicsToMirror(
                        coordinatorNode.id(), existingTopics, new AddTopicsToMirrorOptions());
                addResult.all().get();

                System.out.printf("Added %d topic(s) to mirror %s: %s%n", createdTopics.size(), mirrorName, createdTopics);
            }
        }

        public void removeTopicsFromMirror(MirrorCommandOptions opts) throws Exception {
            String topicPattern = opts.topic().get();
            String mirrorName = opts.mirror().get();

            Node node = findCoordinator(mirrorName);
            String bootstrapServer = node.host() + ":" + node.port();

            Set<String> matchingTopics;

            try (Admin admin = createAdminClient(Optional.of(bootstrapServer), commandConfig)) {
                // List all topics from destination cluster and match against the pattern
                var allTopics = admin.listTopics().names().get();
                matchingTopics = matchTopics(allTopics, topicPattern);

                // Remove all matching topics from the mirror
                RemoveTopicsFromMirrorResult removeTopicsFromMirrorResult = admin.removeTopicsFromMirror(
                    matchingTopics, new RemoveTopicsFromMirrorOptions());
                removeTopicsFromMirrorResult.all().get();
                System.out.printf("Removed %d topic(s) from mirror %s: %s%n", matchingTopics.size(), mirrorName, matchingTopics);
            }
        }

        public void listMirrors() throws ExecutionException, InterruptedException {
            ListMirrorsResult result = adminClient.listMirrors();
            List<MirrorListing> listings = new ArrayList<>(result.all().get());

            if (listings.isEmpty()) {
                System.out.println("No mirrors found");
                return;
            }

            // Sort by mirror name
            listings.sort(Comparator.comparing(MirrorListing::mirrorName));

            // Print header
            System.out.printf("%-30s %-10s %-50s%n", "MIRROR", "TOPICS", "SOURCE-BOOTSTRAP");

            // Print each mirror
            for (MirrorListing listing : listings) {
                String sourceBootstrap = listing.sourceBootstrap() != null && !listing.sourceBootstrap().isEmpty()
                    ? listing.sourceBootstrap()
                    : "-";
                System.out.printf("%-30s %-10d %-50s%n",
                    truncateLeft(listing.mirrorName(), 30),
                    listing.topicCount(),
                    truncateLeft(sourceBootstrap, 50));
            }
        }

        public void describeMirrors(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            // If --mirror is specified, describe only that mirror; otherwise describe all mirrors
            List<String> mirrorNames = opts.mirror().isPresent()
                ? List.of(opts.mirror().get())
                : List.of();

            // Describe the mirror(s) using the admin client
            DescribeMirrorsResult result = adminClient.describeMirrors(
                mirrorNames,
                new DescribeMirrorsOptions()
            );

            Map<String, MirrorDescription> descriptions = result.allDescriptions().get();

            if (descriptions.isEmpty()) {
                System.out.println("No mirror partitions found");
                return;
            }

            // Collect partition states from all mirrors (one entry per mirror+topic+partition)
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            for (Map.Entry<String, MirrorDescription> mirrorEntry : descriptions.entrySet()) {
                String mirrorName = mirrorEntry.getKey();
                MirrorDescription description = mirrorEntry.getValue();

                for (Map.Entry<String, Set<MirrorDescription.LeaderState>> topicEntry : description.topics().entrySet()) {
                    String topic = topicEntry.getKey();
                    for (MirrorDescription.LeaderState state : topicEntry.getValue()) {
                        partitionInfos.add(new PartitionInfo(
                            mirrorName,
                            topic,
                            state.topicPartition().partition(),
                            state.sourceOffset(),
                            state.destinationOffset(),
                            state.lag(),
                            state.state()
                        ));
                    }
                }
            }

            // Sort by mirror, then topic, then partition
            partitionInfos.sort(Comparator.comparing(PartitionInfo::mirror)
                .thenComparing(PartitionInfo::topic)
                .thenComparing(PartitionInfo::partition));

            // Only print header and results if there are partitions to display
            if (!partitionInfos.isEmpty()) {
                System.out.printf("%-30s %-40s %-10s %-15s %-18s %-10s %-12s%n",
                    "MIRROR", "TOPIC", "PARTITION", "SOURCE-OFFSET", "DESTINATION-OFFSET", "LAG", "STATE");
                for (PartitionInfo info : partitionInfos) {
                    System.out.printf("%-30s %-40s %-10d %-15d %-18d %-10d %-12s%n",
                        truncateLeft(info.mirror, 30),
                        truncateLeft(info.topic, 40),
                        info.partition,
                        info.sourceOffset,
                        info.destinationOffset,
                        info.lag,
                        info.state);
                }
            }
        }

        // Truncate string from the left, keeping the rightmost characters
        // Example: truncateLeft("my-very-long-mirror-name", 10) -> "...or-name"
        private String truncateLeft(String str, int maxLength) {
            if (str.length() <= maxLength) {
                return str;
            }
            return "..." + str.substring(str.length() - (maxLength - 3));
        }

        @Override
        public void close() throws Exception {
            adminClient.close();
        }

        // Helper class for formatting partition information
        // Each partition is an independent replication unit with its own state
        private static class PartitionInfo {
            private final String mirror;
            private final String topic;
            private final int partition;
            private final long sourceOffset;
            private final long destinationOffset;
            private final long lag;
            private final String state;

            PartitionInfo(String mirror, String topic, int partition, long sourceOffset,
                          long destinationOffset, long lag, String state) {
                this.mirror = mirror;
                this.topic = topic;
                this.partition = partition;
                this.sourceOffset = sourceOffset;
                this.destinationOffset = destinationOffset;
                this.lag = lag;
                this.state = state;
            }

            String mirror() {
                return mirror;
            }

            String topic() {
                return topic;
            }

            int partition() {
                return partition;
            }
        }
    }

    public static final class MirrorCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorConfigOpt;
        private final OptionSpecBuilder createOpt;
        private final OptionSpecBuilder addOpt;
        private final OptionSpecBuilder removeOpt;
        private final OptionSpecBuilder listOpt;
        private final OptionSpecBuilder describeOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorOpt;
        private final ArgumentAcceptingOptionSpec<String> topicOpt;
        private final ArgumentAcceptingOptionSpec<Short> replicationFactorOpt;

        public MirrorCommandOptions(String[] args) {
            super(args);

            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The destination Kafka server to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);

            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
                .withRequiredArg()
                .describedAs("command config property file")
                .ofType(String.class);

            mirrorConfigOpt = parser.accepts("mirror-config", "Property file containing source cluster configs for mirroring.")
                .withRequiredArg()
                .describedAs("mirror config property file")
                .ofType(String.class);

            createOpt = parser.accepts("create", "Create a new cluster mirror from a source cluster.");
            addOpt = parser.accepts("add", "Add topic(s) to an existing cluster mirror (supports regex).");
            removeOpt = parser.accepts("remove", "Remove topic(s) from an existing cluster mirror (supports regex).");
            listOpt = parser.accepts("list", "List all cluster mirrors.");
            describeOpt = parser.accepts("describe", "Describe a cluster mirror including partition lag and state.");

            mirrorOpt = parser.accepts("mirror", "The name of the cluster mirror.")
                .withRequiredArg()
                .describedAs("mirror")
                .ofType(String.class);

            topicOpt = parser.accepts("topic", "Topic name or regex pattern to match topics (e.g., 'my-topic' or 'test-.*').")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);

            replicationFactorOpt = parser.accepts("replication-factor", "The replication factor to use for the mirror topic. If not specified, uses the destination cluster's default.")
                .withRequiredArg()
                .describedAs("replication-factor")
                .ofType(Short.class);

            options = parser.parse(args);
            checkArgs();
        }

        public boolean has(OptionSpec<?> builder) {
            return options.has(builder);
        }

        public <A> Optional<A> valueAsOption(OptionSpec<A> option) {
            return options.has(option) ? Optional.of(options.valueOf(option)) : Optional.empty();
        }

        public boolean hasCreateOption() {
            return has(createOpt);
        }

        public boolean hasAddOption() {
            return has(addOpt);
        }

        public boolean hasRemoveOption() {
            return has(removeOpt);
        }

        public boolean hasListOption() {
            return has(listOpt);
        }

        public boolean hasDescribeOption() {
            return has(describeOpt);
        }

        public Optional<String> bootstrapServer() {
            return valueAsOption(bootstrapServerOpt);
        }

        public Properties commandConfig() throws IOException {
            if (has(commandConfigOpt)) {
                return Utils.loadProps(options.valueOf(commandConfigOpt));
            } else {
                return new Properties();
            }
        }

        public Properties mirrorConfig() throws IOException {
            if (has(mirrorConfigOpt)) {
                return Utils.loadProps(options.valueOf(mirrorConfigOpt));
            } else {
                return new Properties();
            }
        }

        public Optional<String> mirror() {
            return valueAsOption(mirrorOpt);
        }

        public Optional<String> topic() {
            return valueAsOption(topicOpt);
        }

        public Optional<Short> replicationFactor() {
            return valueAsOption(replicationFactorOpt);
        }

        @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
        private void checkArgs() {
            if (args.length == 0)
                CommandLineUtils.printUsageAndExit(parser, "Create cluster mirrors and add topics to them.");

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to create cluster mirrors and add topics to them.");

            // should have exactly one action
            if ((has(createOpt) ? 1 : 0) + (has(addOpt) ? 1 : 0) + (has(removeOpt) ? 1 : 0)
                    + (has(listOpt) ? 1 : 0) + (has(describeOpt) ? 1 : 0) != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --create, --add, --remove, --list, or --describe");

            // check required args
            if (!has(bootstrapServerOpt))
                throw new IllegalArgumentException("--bootstrap-server must be specified");

            // --mirror is required for create, add, and remove operations, but optional for list and describe
            if (!has(listOpt) && !has(describeOpt) && !has(mirrorOpt))
                throw new IllegalArgumentException("--mirror must be specified");

            if (has(createOpt) && !has(mirrorConfigOpt))
                throw new IllegalArgumentException("--mirror-config must be specified when creating a mirror");

            if (has(addOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when adding topic(s) to a mirror");

            if (has(removeOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when removing topic(s) from a mirror");
        }
    }
}
