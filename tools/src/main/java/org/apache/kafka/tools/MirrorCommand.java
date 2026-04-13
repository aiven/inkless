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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateMirrorOptions;
import org.apache.kafka.clients.admin.CreateMirrorResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteMirrorOptions;
import org.apache.kafka.clients.admin.DeleteMirrorResult;
import org.apache.kafka.clients.admin.DescribeMirrorsOptions;
import org.apache.kafka.clients.admin.DescribeMirrorsResult;
import org.apache.kafka.clients.admin.ListMirrorsResult;
import org.apache.kafka.clients.admin.MirrorDescription;
import org.apache.kafka.clients.admin.MirrorListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PauseMirrorTopicsOptions;
import org.apache.kafka.clients.admin.PauseMirrorTopicsResult;
import org.apache.kafka.clients.admin.RemoveTopicsFromMirrorOptions;
import org.apache.kafka.clients.admin.RemoveTopicsFromMirrorResult;
import org.apache.kafka.clients.admin.ResumeMirrorTopicsOptions;
import org.apache.kafka.clients.admin.ResumeMirrorTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

/**
 * Command-line tool for managing cluster mirrors.
 */
public abstract class MirrorCommand {
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
        try (MirrorService mirrorService = new MirrorService(opts.bootstrapServer(), opts.commandConfig(), opts.mirrorConfig())) {
            if (opts.hasCreateOption()) {
                mirrorService.createMirror(opts);
            } else if (opts.hasAlterOption()) {
                mirrorService.alterMirror(opts);
            } else if (opts.hasAddOption()) {
                mirrorService.addTopicsToMirror(opts);
            } else if (opts.hasRemoveOption()) {
                mirrorService.removeTopicsFromMirror(opts);
            } else if (opts.hasDeleteOption()) {
                mirrorService.deleteMirror(opts);
            } else if (opts.hasPauseOption()) {
                mirrorService.pauseMirrorTopics(opts);
            } else if (opts.hasResumeOption()) {
                mirrorService.resumeMirrorTopics(opts);
            } else if (opts.hasListOption()) {
                mirrorService.listMirrors();
            } else if (opts.hasDescribeOption()) {
                mirrorService.describeMirrors(opts);
            }
        }
    }

    private static class MirrorService implements AutoCloseable {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private final Admin adminClient;
        private final Properties mirrorConfigs;

        MirrorService(Optional<String> bootstrapServer, Properties commandConfig, Properties mirrorConfigs) {
            this.adminClient = createAdminClient(bootstrapServer, commandConfig);
            this.mirrorConfigs = mirrorConfigs;
        }

        private static Admin createAdminClient(Optional<String> bootstrapServer, Properties commandConfig) {
            bootstrapServer.ifPresent(s -> commandConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s));
            return Admin.create(commandConfig);
        }

        private Set<String> matchTopics(Set<String> allTopics, String topicPattern) {
            Set<String> matchingTopics = new HashSet<>();
            Pattern pattern = Pattern.compile(topicPattern);
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

        private void createMirror(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
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

        private void alterMirror(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.MIRROR, opts.mirror().get());
            List<AlterConfigOp> ops = new ArrayList<>();
            mirrorConfigs.forEach((k, v) ->
                ops.add(new AlterConfigOp(
                    new ConfigEntry(k.toString(), v.toString()),
                    AlterConfigOp.OpType.SET)));

            Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(resource, ops);
            adminClient.incrementalAlterConfigs(configs).all().get();
            System.out.printf("Altered mirror %s%n", opts.mirror().get());
        }

        private void addTopicsToMirror(MirrorCommandOptions opts) throws Exception {
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
            CreateTopicsResult createResult = adminClient.createTopics(newTopics,
                new CreateTopicsOptions().retryOnQuotaViolation(false));

            // Attach created and existing topic to the mirror
            Map<String, String> topics = new HashMap<>();
            for (String topicName : matchingTopics) {
                try {
                    createResult.values().get(topicName).get();
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        System.err.printf("Failed to add topic %s: %s%n", topicName, e.getCause().getMessage());
                    }
                } finally {
                    topics.put(topicName, mirrorName);
                }
            }

            // TODO: We should return error and let the command retry if the topic metadata is not propagated to brokers. Right now, we sleep 1 sec
            Thread.sleep(1000);
            // Ensures the mirror.name config is properly set even when topics already exist
            AddTopicsToMirrorResult addResult = adminClient.addTopicsToMirror(mirrorName, topics.keySet(), new AddTopicsToMirrorOptions());
            addResult.all().get();

            System.out.printf("Added %d topic(s) to mirror %s: %s%n", topics.size(), mirrorName, topics.keySet());
        }

        private void removeTopicsFromMirror(MirrorCommandOptions opts) throws Exception {
            String topicPattern = opts.topic().get();
            String mirrorName = opts.mirror().get();

            Set<String> matchingTopics;

            // List all topics from destination cluster and match against the pattern
            var allTopics = adminClient.listTopics().names().get();
            matchingTopics = matchTopics(allTopics, topicPattern);

            // Remove all matching topics from the mirror
            RemoveTopicsFromMirrorResult removeTopicsFromMirrorResult = adminClient.removeTopicsFromMirror(
                mirrorName, matchingTopics, new RemoveTopicsFromMirrorOptions());
            removeTopicsFromMirrorResult.all().get();
            System.out.printf("Removed %d topic(s) from mirror %s: %s%n", matchingTopics.size(), mirrorName, matchingTopics);
        }

        private void deleteMirror(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            String mirrorName = opts.mirror().get();
            DeleteMirrorResult result = adminClient.deleteMirror(
                mirrorName, new DeleteMirrorOptions());
            result.all().get();
            System.out.printf("Deleted mirror %s%n", mirrorName);
        }

        private void pauseMirrorTopics(MirrorCommandOptions opts) throws Exception {
            String topicPattern = opts.topic().get();
            String mirrorName = opts.mirror().get();

            var allTopics = adminClient.listTopics().names().get();
            Set<String> matchingTopics = matchTopics(allTopics, topicPattern);

            PauseMirrorTopicsResult result = adminClient.pauseMirrorTopics(mirrorName, matchingTopics, new PauseMirrorTopicsOptions());
            result.all().get();
            System.out.printf("Paused mirroring for %d topic(s) in mirror %s: %s%n", matchingTopics.size(), mirrorName, matchingTopics);
        }

        private void resumeMirrorTopics(MirrorCommandOptions opts) throws Exception {
            String topicPattern = opts.topic().get();
            String mirrorName = opts.mirror().get();

            var allTopics = adminClient.listTopics().names().get();
            Set<String> matchingTopics = matchTopics(allTopics, topicPattern);

            ResumeMirrorTopicsResult result = adminClient.resumeMirrorTopics(mirrorName, matchingTopics, new ResumeMirrorTopicsOptions());
            result.all().get();
            System.out.printf("Resumed mirroring for %d topic(s) in mirror %s: %s%n", matchingTopics.size(), mirrorName, matchingTopics);
        }

        private void listMirrors() throws ExecutionException, InterruptedException {
            ListMirrorsResult result = adminClient.listMirrors();
            List<MirrorListing> listings = new ArrayList<>(result.all().get());

            if (listings.isEmpty()) {
                System.out.println("No mirrors found");
                return;
            }

            // Sort by mirror name
            listings.sort(Comparator.comparing(MirrorListing::mirrorName));

            // Print header
            System.out.printf("%-30s %-10s %-26s %-50s%n", "MIRROR", "TOPICS", "SOURCE-CLUSTER-ID", "SOURCE-BOOTSTRAP");

            // Print each mirror
            for (MirrorListing listing : listings) {
                String sourceBootstrap = listing.sourceBootstrap() != null && !listing.sourceBootstrap().isEmpty()
                    ? listing.sourceBootstrap()
                    : "-";
                String sourceClusterId = listing.sourceClusterId() != null && !listing.sourceClusterId().isEmpty()
                    ? listing.sourceClusterId()
                    : "-";
                System.out.printf("%-30s %-10d %-26s %-50s%n",
                    truncateLeft(listing.mirrorName(), 30),
                    listing.topicCount(),
                    sourceClusterId,
                    truncateLeft(sourceBootstrap, 50));
            }
        }

        private void describeMirrors(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
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
                            state.state(),
                            state.lastMirrorEpoch()
                        ));
                    }
                }
            }

            // Sort by mirror, then topic, then partition
            partitionInfos.sort(Comparator.comparing(PartitionInfo::mirror)
                .thenComparing(PartitionInfo::topic)
                .thenComparing(PartitionInfo::partition));

            if (opts.hasJsonOption()) {
                try {
                    System.out.printf(OBJECT_MAPPER.writeValueAsString(partitionInfos));
                    return;
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to serialize JSON", e);
                }
            }

            // Only print header and results if there are partitions to display
            if (!partitionInfos.isEmpty()) {
                System.out.printf("%-30s %-40s %-10s %-15s %-18s %-10s %-12s%n",
                    "MIRROR", "TOPIC", "PARTITION", "SOURCE-OFFSET", "DESTINATION-OFFSET", "LAG", "STATE");
                for (PartitionInfo info : partitionInfos) {
                    System.out.printf("%-30s %-40s %-10d %-15s %-18s %-10s %-12s%n",
                        truncateLeft(info.mirror(), 30),
                        truncateLeft(info.topic(), 40),
                        info.partition(),
                        formatOffset(info.sourceOffset()),
                        formatOffset(info.destinationOffset()),
                        formatOffset(info.lag()),
                        info.state());
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

        // Format offset value, displaying "-" for unavailable offsets (-1)
        private String formatOffset(long offset) {
            return offset == -1 ? "-" : String.valueOf(offset);
        }

        @Override
        public void close() throws Exception {
            adminClient.close();
        }

        private record PartitionInfo(String mirror, String topic, int partition,
                                     long sourceOffset, long destinationOffset, long lag, String state, int lastMirrorEpoch) { }
    }

    private static final class MirrorCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorConfigOpt;
        private final OptionSpecBuilder createOpt;
        private final OptionSpecBuilder alterOpt;
        private final OptionSpecBuilder addOpt;
        private final OptionSpecBuilder removeOpt;
        private final OptionSpecBuilder deleteOpt;
        private final OptionSpecBuilder pauseOpt;
        private final OptionSpecBuilder resumeOpt;
        private final OptionSpecBuilder listOpt;
        private final OptionSpecBuilder describeOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorOpt;
        private final ArgumentAcceptingOptionSpec<String> topicOpt;
        private final ArgumentAcceptingOptionSpec<Short> replicationFactorOpt;
        private final OptionSpecBuilder jsonOpt;

        MirrorCommandOptions(String[] args) {
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
            alterOpt = parser.accepts("alter", "Alter the configuration of an existing cluster mirror.");
            addOpt = parser.accepts("add", "Add topic(s) to an existing cluster mirror (supports regex).");
            removeOpt = parser.accepts("remove", "Remove topic(s) from an existing cluster mirror (supports regex).");
            deleteOpt = parser.accepts("delete", "Delete a cluster mirror.");
            pauseOpt = parser.accepts("pause", "Pause mirroring for topic(s) matching the pattern (supports regex).");
            resumeOpt = parser.accepts("resume", "Resume mirroring for previously paused topic(s) matching the pattern (supports regex).");
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

            replicationFactorOpt = parser.accepts("replication-factor", "The replication factor to use for the mirror topic. " +
                            "If not specified, uses the destination cluster's default.")
                .withRequiredArg()
                .describedAs("replication-factor")
                .ofType(Short.class);

            jsonOpt = parser.accepts("json", "Output description in JSON format");

            options = parser.parse(args);
            checkArgs();
        }

        private boolean has(OptionSpec<?> builder) {
            return options.has(builder);
        }

        private <A> Optional<A> valueAsOption(OptionSpec<A> option) {
            return options.has(option) ? Optional.of(options.valueOf(option)) : Optional.empty();
        }

        private boolean hasCreateOption() {
            return has(createOpt);
        }

        private boolean hasAlterOption() {
            return has(alterOpt);
        }

        private boolean hasAddOption() {
            return has(addOpt);
        }

        private boolean hasRemoveOption() {
            return has(removeOpt);
        }

        private boolean hasDeleteOption() {
            return has(deleteOpt);
        }

        private boolean hasPauseOption() {
            return has(pauseOpt);
        }

        private boolean hasResumeOption() {
            return has(resumeOpt);
        }

        private boolean hasListOption() {
            return has(listOpt);
        }

        private boolean hasJsonOption() {
            return has(jsonOpt);
        }

        private boolean hasDescribeOption() {
            return has(describeOpt);
        }

        private Optional<String> bootstrapServer() {
            return valueAsOption(bootstrapServerOpt);
        }

        private Properties commandConfig() throws IOException {
            if (has(commandConfigOpt)) {
                return Utils.loadProps(options.valueOf(commandConfigOpt));
            } else {
                return new Properties();
            }
        }

        private Properties mirrorConfig() throws IOException {
            if (has(mirrorConfigOpt)) {
                return Utils.loadProps(options.valueOf(mirrorConfigOpt));
            } else {
                return new Properties();
            }
        }

        private Optional<String> mirror() {
            return valueAsOption(mirrorOpt);
        }

        private Optional<String> topic() {
            return valueAsOption(topicOpt);
        }

        private Optional<Short> replicationFactor() {
            return valueAsOption(replicationFactorOpt);
        }

        @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
        private void checkArgs() {
            if (args.length == 0)
                CommandLineUtils.printUsageAndExit(parser, "Create cluster mirrors and add topics to them.");

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to create cluster mirrors and add topics to them.");

            // should have exactly one action
            if ((has(createOpt) ? 1 : 0) + (has(alterOpt) ? 1 : 0) + (has(addOpt) ? 1 : 0) + (has(removeOpt) ? 1 : 0)
                    + (has(deleteOpt) ? 1 : 0) + (has(pauseOpt) ? 1 : 0) + (has(resumeOpt) ? 1 : 0)
                    + (has(listOpt) ? 1 : 0) + (has(describeOpt) ? 1 : 0) != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --create, --alter, --add, " +
                        "--remove, --delete, --pause, --resume, --list, or --describe");

            // check required args
            if (!has(bootstrapServerOpt))
                throw new IllegalArgumentException("--bootstrap-server must be specified");

            // --mirror is required for create, alter, add, remove, pause, and resume operations, but optional for list and describe
            if (!has(listOpt) && !has(describeOpt) && !has(mirrorOpt))
                throw new IllegalArgumentException("--mirror must be specified");

            if (has(createOpt) && !has(mirrorConfigOpt))
                throw new IllegalArgumentException("--mirror-config must be specified when creating a mirror");

            if (has(alterOpt) && !has(mirrorConfigOpt))
                throw new IllegalArgumentException("--mirror-config must be specified when altering a mirror");

            if (has(addOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when adding topic(s) to a mirror");

            if (has(removeOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when removing topic(s) from a mirror");

            if (has(pauseOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when pausing mirror topic(s)");

            if (has(resumeOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when resuming mirror topic(s)");

            if (has(jsonOpt) && !has(describeOpt))
                throw new IllegalArgumentException("--json is only supported for describing mirrors");
        }
    }
}
