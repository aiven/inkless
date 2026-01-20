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
import org.apache.kafka.clients.admin.FindCoordinatorResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RemoveTopicsFromMirrorOptions;
import org.apache.kafka.clients.admin.RemoveTopicsFromMirrorResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
            System.out.printf("Found coordinator %s for mirror %s%n", coordinator.idString(), mirrorName);
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
            System.out.printf("Found %d topic(s) matching pattern '%s': %s%n", matchingTopics.size(), topicPattern, matchingTopics);
            return matchingTopics;
        }

        public void createMirror(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            System.out.printf("Creating mirror %s%n", opts.mirror().get());

            Map<String, String> configMap = new HashMap<>();
            mirrorConfigs.forEach((k, v) -> configMap.put(k.toString(), v.toString()));

            CreateMirrorResult result = adminClient.createMirror(
                opts.mirror().get(),
                configMap,
                new CreateMirrorOptions()
            );
            result.all().get();
            System.out.printf("Successfully created mirror %s%n", opts.mirror().get());
        }

        public void addTopicsToMirror(MirrorCommandOptions opts) throws Exception {
            String topicPattern = opts.topic().get();
            String mirrorName = opts.mirror().get();

            // Connect to source cluster and get matching topics
            Set<String> matchingTopics;
            Map<String, String> topicIds = new HashMap<>();
            Map<String, Integer> partNums = new HashMap<>();
            Map<String, Integer> replicationFactors = new HashMap<>();

            try (Admin sourceAdmin = Admin.create(mirrorConfigs)) {
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
                    replicationFactors.put(topic, desc.partitions().get(0).replicas().size());
                }
            }

            Node node = findCoordinator(mirrorName);
            String bootstrapServer = node.host() + ":" + node.port();

            try (Admin admin = createAdminClient(Optional.of(bootstrapServer), commandConfig)) {
                // Prepare all NewTopic objects for batch creation
                Set<NewTopic> newTopics = new HashSet<>();
                for (String topicName : matchingTopics) {
                    String topicId = topicIds.get(topicName);
                    int partNum = partNums.get(topicName);
                    int replicationFactor = replicationFactors.get(topicName);
                    NewTopic newTopic = new NewTopic(topicName,
                        Optional.of(partNum), // use source topic partitions
                        Optional.of((short) replicationFactor),
                        Optional.of(mirrorName),
                        Optional.of(topicId));
                    newTopics.add(newTopic);
                }

                System.out.printf("Adding %d topic(s) to mirror %s%n", newTopics.size(), mirrorName);

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
                        if (e.getCause() instanceof TopicExistsException) {
                            existingTopics.put(topicName, mirrorName);
                        } else {
                            System.err.printf("Failed to add topic %s: %s%n", topicName, e.getCause().getMessage());
                        }
                    }
                }

                if (!createdTopics.isEmpty()) {
                    System.out.printf("Successfully added %d topic(s) to mirror %s: %s%n",
                        createdTopics.size(), mirrorName, createdTopics);
                }

                if (!existingTopics.isEmpty()) {
                    AddTopicsToMirrorResult addResult = admin.addTopicsToMirror(
                        existingTopics, new AddTopicsToMirrorOptions());
                    addResult.all().get();
                    System.out.printf("Successfully added %d existing topic(s) to mirror %s%n",
                        existingTopics.size(), mirrorName);
                }
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
                    mirrorName, matchingTopics, new RemoveTopicsFromMirrorOptions());
                removeTopicsFromMirrorResult.all().get();
                System.out.printf("Successfully removed %d topic(s) from mirror %s%n", matchingTopics.size(), mirrorName);
            }
        }

        @Override
        public void close() throws Exception {
            adminClient.close();
        }
    }

    public static final class MirrorCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorConfigOpt;
        private final OptionSpecBuilder createOpt;
        private final OptionSpecBuilder addOpt;
        private final OptionSpecBuilder removeOpt;
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
            long actions = (has(createOpt) ? 1 : 0) + (has(addOpt) ? 1 : 0) + (has(removeOpt) ? 1 : 0);
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --create or --add");

            // check required args
            if (!has(bootstrapServerOpt))
                throw new IllegalArgumentException("--bootstrap-server must be specified");

            if (!has(mirrorOpt))
                throw new IllegalArgumentException("--mirror must be specified");

            if (has(createOpt) && !has(mirrorConfigOpt))
                throw new IllegalArgumentException("--mirror-config must be specified when creating a mirror");

            if (has(addOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when adding topic(s) to a mirror");

            if (has(addOpt) && !has(mirrorConfigOpt))
                throw new IllegalArgumentException("--mirror-config must be specified when adding topic(s) to a mirror");

            if (has(removeOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when removing topic(s) from a mirror");
        }
    }
}
