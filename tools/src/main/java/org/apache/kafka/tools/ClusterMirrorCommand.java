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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ClusterMirrorDesc;
import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.clients.admin.CreateClusterMirrorOptions;
import org.apache.kafka.clients.admin.CreateClusterMirrorResult;
import org.apache.kafka.clients.admin.DeleteClusterMirrorOptions;
import org.apache.kafka.clients.admin.DeleteClusterMirrorResult;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsOptions;
import org.apache.kafka.clients.admin.ListClusterMirrorsResult;
import org.apache.kafka.clients.admin.PauseMirrorTopicsOptions;
import org.apache.kafka.clients.admin.PauseMirrorTopicsResult;
import org.apache.kafka.clients.admin.ResumeMirrorTopicsOptions;
import org.apache.kafka.clients.admin.ResumeMirrorTopicsResult;
import org.apache.kafka.clients.admin.StartMirrorTopicsOptions;
import org.apache.kafka.clients.admin.StopMirrorTopicsOptions;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.MirrorFilterUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

/**
 * Command-line tool for managing cluster mirrors.
 */
public abstract class ClusterMirrorCommand {
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
                mirrorService.createClusterMirror(opts);
            } else if (opts.hasStartOption()) {
                mirrorService.startMirrorTopics(opts);
            } else if (opts.hasStopOption()) {
                mirrorService.stopMirrorTopics(opts);
            } else if (opts.hasDeleteOption()) {
                mirrorService.deleteClusterMirror(opts);
            } else if (opts.hasPauseOption()) {
                mirrorService.pauseMirrorTopics(opts);
            } else if (opts.hasResumeOption()) {
                mirrorService.resumeMirrorTopics(opts);
            } else if (opts.hasListOption()) {
                mirrorService.listClusterMirrors();
            } else if (opts.hasDescribeOption()) {
                mirrorService.describeClusterMirrors(opts);
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

        private void createClusterMirror(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            Map<String, String> configMap = new HashMap<>();
            mirrorConfigs.forEach((k, v) -> configMap.put(k.toString(), v.toString()));

            CreateClusterMirrorResult result = adminClient.createClusterMirror(
                opts.mirror().get(),
                configMap,
                new CreateClusterMirrorOptions()
            );
            result.all().get();
            System.out.printf("Created mirror %s%n", opts.mirror().get());
        }

        private void startMirrorTopics(MirrorCommandOptions opts) throws Exception {
            String mirrorName = opts.mirror().get();
            List<String> topicPatterns = opts.topics();
            List<String> excludePatterns = opts.exclude();

            var mirrorConfigEntries = describeMirrorConfig(mirrorName);
            Properties sourceConfig = toProperties(mirrorConfigEntries);

            Set<String> matchingTopicNames;
            try (Admin sourceAdmin = Admin.create(sourceConfig)) {
                Set<String> allSourceTopics = sourceAdmin.listTopics().names().get();
                Pattern includePattern = MirrorFilterUtils.compilePatternList(topicPatterns);
                Pattern excludePattern = MirrorFilterUtils.compilePatternList(excludePatterns);
                matchingTopicNames = allSourceTopics.stream()
                        .filter(t -> includePattern != null && includePattern.matcher(t).matches())
                        .filter(t -> excludePattern == null || !excludePattern.matcher(t).matches())
                        .collect(Collectors.toSet());
            }

            adminClient.startMirrorTopics(mirrorName, matchingTopicNames,
                    new StartMirrorTopicsOptions()
                            .includePatterns(topicPatterns)
                            .excludePatterns(excludePatterns))
                    .all().get();
            if (!matchingTopicNames.isEmpty()) {
                System.out.printf("Started %d mirror topic(s) in mirror %s: %s%n",
                        matchingTopicNames.size(), mirrorName, matchingTopicNames);
            } else {
                System.out.printf("No matching topics found yet. Patterns saved to mirror %s for auto-discovery.%n", mirrorName);
            }
        }

        private void stopMirrorTopics(MirrorCommandOptions opts) throws Exception {
            String mirrorName = opts.mirror().get();
            List<String> patterns = opts.topics();

            // Resolve topics on destination that match the patterns
            Set<String> topics = resolveTopicsOnDestination(patterns);

            adminClient.stopMirrorTopics(mirrorName, topics,
                    new StopMirrorTopicsOptions().patterns(patterns))
                    .all().get();
            System.out.printf("Stopped mirroring for topics %s in mirror %s%n", topics, mirrorName);
        }

        private org.apache.kafka.clients.admin.Config describeMirrorConfig(String mirrorName) throws Exception {
            ConfigResource mirrorConfigResource = new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName);
            var configResult = adminClient.describeConfigs(List.of(mirrorConfigResource)).all().get();
            var mirrorConfigEntries = configResult.get(mirrorConfigResource);

            if (mirrorConfigEntries == null || mirrorConfigEntries.entries().isEmpty()) {
                throw new RuntimeException("Mirror '" + mirrorName + "' not found or has no configuration");
            }
            return mirrorConfigEntries;
        }

        private static Properties toProperties(org.apache.kafka.clients.admin.Config config) {
            Properties props = new Properties();
            for (var entry : config.entries()) {
                props.put(entry.name(), entry.value());
            }
            return props;
        }

        private void pauseMirrorTopics(MirrorCommandOptions opts) throws Exception {
            String mirrorName = opts.mirror().get();
            Set<String> topics = resolveTopicsOnDestination(opts.topics());

            PauseMirrorTopicsResult result = adminClient.pauseMirrorTopics(mirrorName, topics, new PauseMirrorTopicsOptions());
            result.all().get();
            System.out.printf("Paused mirroring for %d topic(s) in mirror %s: %s%n", topics.size(), mirrorName, topics);
        }

        private void resumeMirrorTopics(MirrorCommandOptions opts) throws Exception {
            String mirrorName = opts.mirror().get();
            Set<String> topics = resolveTopicsOnDestination(opts.topics());

            ResumeMirrorTopicsResult result = adminClient.resumeMirrorTopics(mirrorName, topics, new ResumeMirrorTopicsOptions());
            result.all().get();
            System.out.printf("Resumed mirroring for %d topic(s) in mirror %s: %s%n", topics.size(), mirrorName, topics);
        }

        private Set<String> resolveTopicsOnDestination(List<String> patterns) throws Exception {
            Set<String> allTopics = adminClient.listTopics().names().get();
            Pattern compiled = MirrorFilterUtils.compilePatternList(patterns);
            if (compiled == null) return Set.of();
            return allTopics.stream()
                    .filter(t -> compiled.matcher(t).matches())
                    .collect(Collectors.toSet());
        }

        private void listClusterMirrors() throws ExecutionException, InterruptedException {
            ListClusterMirrorsResult result = adminClient.listClusterMirrors();
            List<ClusterMirrorListing> listings = new ArrayList<>(result.all().get());

            if (listings.isEmpty()) {
                System.out.println("No mirrors found");
                return;
            }

            // Sort by mirror name
            listings.sort(Comparator.comparing(ClusterMirrorListing::mirrorName));

            // Print header
            System.out.printf("%-30s %-10s %-26s %-50s%n", "MIRROR", "TOPICS", "SOURCE-CLUSTER-ID", "SOURCE-BOOTSTRAP");

            // Print each mirror
            for (ClusterMirrorListing listing : listings) {
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

        private void describeClusterMirrors(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            List<String> mirrorNames = opts.mirror().isPresent()
                ? List.of(opts.mirror().get())
                : List.of();

            Map<String, ClusterMirrorDesc> descriptions = adminClient.describeClusterMirrors(
                mirrorNames, new DescribeClusterMirrorsOptions()).allDescriptions().get();

            if (descriptions.isEmpty()) {
                if (opts.hasJsonOption()) {
                    System.out.print("[]");
                } else {
                    System.out.println("No mirror partitions found");
                }
                return;
            }

            List<PartitionInfo> partitionInfos = collectPartitionInfos(descriptions);

            if (opts.hasFailedOption()) {
                partitionInfos.removeIf(info -> !"FAILED".equals(info.state()));
            }

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

            if (partitionInfos.isEmpty()) {
                if (opts.hasFailedOption()) {
                    System.out.println("No failed partitions found");
                }
                return;
            }

            if (opts.hasFailedOption()) {
                printFailedPartitions(partitionInfos, getMaxRetryAttempts());
            } else {
                printPartitions(partitionInfos);
            }
        }

        private List<PartitionInfo> collectPartitionInfos(Map<String, ClusterMirrorDesc> descriptions) {
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            for (Map.Entry<String, ClusterMirrorDesc> mirrorEntry : descriptions.entrySet()) {
                String mirrorName = mirrorEntry.getKey();
                for (Map.Entry<String, Set<ClusterMirrorDesc.LeaderStateDesc>> topicEntry : mirrorEntry.getValue().topics().entrySet()) {
                    String topic = topicEntry.getKey();
                    for (ClusterMirrorDesc.LeaderStateDesc state : topicEntry.getValue()) {
                        partitionInfos.add(new PartitionInfo(
                            mirrorName, topic,
                            state.topicPartition().partition(),
                            state.sourceOffset(), state.destinationOffset(), state.lag(),
                            state.state(), state.retryAttempt(), state.errorMessage(),
                            state.lastMirrorEpoch()));
                    }
                }
            }
            return partitionInfos;
        }

        private int getMaxRetryAttempts() {
            try {
                String brokerId = adminClient.describeCluster().nodes().get().iterator().next().idString();
                ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
                var configs = adminClient.describeConfigs(List.of(brokerResource)).all().get();
                var brokerConfig = configs.get(brokerResource);
                if (brokerConfig != null) {
                    var entry = brokerConfig.get("mirror.failed.retry.max.attempts");
                    if (entry != null) {
                        return Integer.parseInt(entry.value());
                    }
                }
            } catch (RuntimeException | InterruptedException | ExecutionException e) {
                // fall through to default
            }
            return 10;
        }

        private void printFailedPartitions(List<PartitionInfo> partitionInfos, int maxRetryAttempts) {
            int maxError = 80;
            System.out.printf("%-30s %-40s %-10s %-7s %s%n",
                "MIRROR", "TOPIC", "PARTITION", "RETRY", "ERROR");
            for (PartitionInfo info : partitionInfos) {
                String error = info.errorMessage() != null ? info.errorMessage() : "";
                if (error.length() > maxError) {
                    error = error.substring(0, maxError - 3) + "...";
                }
                String retry = info.retryAttempt() + "/" + maxRetryAttempts;
                System.out.printf("%-30s %-40s %-10d %-7s %s%n",
                    truncateLeft(info.mirror(), 30),
                    truncateLeft(info.topic(), 40),
                    info.partition(),
                    retry,
                    error);
            }
        }

        private void printPartitions(List<PartitionInfo> partitionInfos) {
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

        private void deleteClusterMirror(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            String mirrorName = opts.mirror().get();
            DeleteClusterMirrorResult result = adminClient.deleteClusterMirror(
                    mirrorName, new DeleteClusterMirrorOptions());
            result.all().get();
            System.out.printf("Deleted mirror %s%n", mirrorName);
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
                                     long sourceOffset, long destinationOffset, long lag,
                                     String state, short retryAttempt, String errorMessage,
                                     int lastMirrorEpoch) { }
    }

    private static final class MirrorCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorConfigOpt;
        private final OptionSpecBuilder createOpt;
        private final OptionSpecBuilder startOpt;
        private final OptionSpecBuilder stopOpt;
        private final OptionSpecBuilder deleteOpt;
        private final OptionSpecBuilder pauseOpt;
        private final OptionSpecBuilder resumeOpt;
        private final OptionSpecBuilder listOpt;
        private final OptionSpecBuilder describeOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorOpt;
        private final ArgumentAcceptingOptionSpec<String> topicsOpt;
        private final ArgumentAcceptingOptionSpec<String> excludeOpt;
        private final OptionSpecBuilder jsonOpt;
        private final OptionSpecBuilder failedOpt;

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
            startOpt = parser.accepts("start", "Start mirroring topics matching the given patterns.");
            stopOpt = parser.accepts("stop", "Stop mirroring topics matching the given patterns.");
            pauseOpt = parser.accepts("pause", "Pause mirroring for topics matching the given patterns.");
            resumeOpt = parser.accepts("resume", "Resume mirroring for previously paused topics matching the given patterns.");
            listOpt = parser.accepts("list", "List all cluster mirrors.");
            describeOpt = parser.accepts("describe", "Describe a cluster mirror including partition lag and state.");
            deleteOpt = parser.accepts("delete", "Delete a cluster mirror.");

            mirrorOpt = parser.accepts("mirror", "The name of the cluster mirror.")
                .withRequiredArg()
                .describedAs("mirror")
                .ofType(String.class);

            topicsOpt = parser.accepts("topics", "Comma-separated list of topic names or regex patterns (e.g., 'my-topic,orders-.*,payments').")
                .withRequiredArg()
                .describedAs("topics")
                .ofType(String.class);

            excludeOpt = parser.accepts("exclude", "Comma-separated list of topic names or regex patterns to exclude from mirroring. " +
                            "Only valid with --start.")
                .withRequiredArg()
                .describedAs("exclude patterns")
                .ofType(String.class);

            jsonOpt = parser.accepts("json", "Output description in JSON format");
            failedOpt = parser.accepts("failed", "Show only failed partitions with error details.");

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

        private boolean hasStartOption() {
            return has(startOpt);
        }

        private boolean hasStopOption() {
            return has(stopOpt);
        }

        private boolean hasPauseOption() {
            return has(pauseOpt);
        }

        private boolean hasResumeOption() {
            return has(resumeOpt);
        }

        private boolean hasDeleteOption() {
            return has(deleteOpt);
        }

        private boolean hasListOption() {
            return has(listOpt);
        }

        private boolean hasJsonOption() {
            return has(jsonOpt);
        }

        private boolean hasFailedOption() {
            return has(failedOpt);
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

        private List<String> topics() {
            if (!has(topicsOpt)) return List.of();
            return Arrays.stream(options.valueOf(topicsOpt).split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }

        private List<String> exclude() {
            if (!has(excludeOpt)) return List.of();
            return Arrays.stream(options.valueOf(excludeOpt).split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }

        @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
        private void checkArgs() {
            if (args.length == 0)
                CommandLineUtils.printUsageAndExit(parser, "Create cluster mirrors and manage mirror topics.");

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to create cluster mirrors and manage mirror topics.");

            // should have exactly one action
            if ((has(createOpt) ? 1 : 0) + (has(startOpt) ? 1 : 0) + (has(stopOpt) ? 1 : 0)
                    + (has(deleteOpt) ? 1 : 0) + (has(pauseOpt) ? 1 : 0) + (has(resumeOpt) ? 1 : 0)
                    + (has(listOpt) ? 1 : 0) + (has(describeOpt) ? 1 : 0) != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --create, --start, " +
                        "--stop, --delete, --pause, --resume, --list, or --describe");

            // check required args
            if (!has(bootstrapServerOpt))
                throw new IllegalArgumentException("--bootstrap-server must be specified");

            // --mirror is required for create, start, stop, pause, resume, and delete operations, but optional for list and describe
            if (!has(listOpt) && !has(describeOpt) && !has(mirrorOpt))
                throw new IllegalArgumentException("--mirror must be specified");

            if (has(createOpt) && !has(mirrorConfigOpt))
                throw new IllegalArgumentException("--mirror-config must be specified when creating a mirror");

            if (has(startOpt) && !has(topicsOpt))
                throw new IllegalArgumentException("--topics must be specified when starting mirror topic(s)");

            if (has(stopOpt) && !has(topicsOpt))
                throw new IllegalArgumentException("--topics must be specified when stopping mirror topic(s)");

            if (has(pauseOpt) && !has(topicsOpt))
                throw new IllegalArgumentException("--topics must be specified when pausing mirror topic(s)");

            if (has(resumeOpt) && !has(topicsOpt))
                throw new IllegalArgumentException("--topics must be specified when resuming mirror topic(s)");

            if (has(excludeOpt) && !has(startOpt))
                throw new IllegalArgumentException("--exclude is only valid with --start");

            if (has(jsonOpt) && !has(describeOpt))
                throw new IllegalArgumentException("--json is only supported for describing mirrors");

            if (has(failedOpt) && !has(describeOpt))
                throw new IllegalArgumentException("--failed is only supported for describing mirrors");
        }
    }
}
