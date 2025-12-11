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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.MirrorConfig;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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
                mirrorService.addTopicToMirror(opts);
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
        private final Map<String, String> mirrorConfigs;
        private final Properties commandConfig;

        public MirrorService(Optional<String> bootstrapServer, Properties commandConfig, Map<String, String> mirrorConfigs) {
            this.adminClient = createAdminClient(bootstrapServer, commandConfig);
            this.commandConfig = commandConfig;
            this.mirrorConfigs = mirrorConfigs;
        }

        private static Admin createAdminClient(Optional<String> bootstrapServer, Properties commandConfig) {
            bootstrapServer.ifPresent(s -> commandConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s));
            return Admin.create(commandConfig);
        }

        public void createMirror(MirrorCommandOptions opts) throws ExecutionException, InterruptedException {
            System.out.println("Creating mirror " + opts.mirror().get());

            // Use the remote bootstrap server from command line option if provided
            Map<String, String> effectiveMirrorConfigs = new HashMap<>(mirrorConfigs);
            opts.remoteBootstrapServer().ifPresent(server ->
                effectiveMirrorConfigs.put(MirrorConfig.BOOTSTRAP_SERVERS_CONFIG, server));

            CreateMirrorResult result = adminClient.createMirror(
                opts.mirror().get(),
                effectiveMirrorConfigs,
                new CreateMirrorOptions()
            );
            result.all().get();
            System.out.println("Successfully created mirror " + opts.mirror().get());
        }

        public void removeTopicsFromMirror(MirrorCommandOptions opts) throws Exception {
            String topicName = opts.topic().get();
            String mirrorName = opts.mirror().get();

            System.out.println("remove topic " + topicName + " from mirror " + mirrorName);


            Optional<Node> coordinator = Optional.empty();
            FindCoordinatorResult findCoordinatorResult = adminClient.findCoordinator(mirrorName);
            coordinator = Optional.ofNullable(findCoordinatorResult.node().get());
            System.out.println("Found coordinator " + coordinator.map(Node::idString).orElse("none") + " for link " + mirrorName + ".");

            if (coordinator.isPresent()) {
                Node node = coordinator.get();
                System.out.println("Node info: " + node);
                String bootstrapServer = node.host() + ":" + node.port();
                System.out.println("Deleting mirror topic " + topicName + " using bootstrap server " + bootstrapServer + ";;" + mirrorName + ".");
                try (Admin admin = createAdminClient(Optional.of(bootstrapServer), commandConfig)) {
                    RemoveTopicsFromMirrorResult deleteMirrorTopicResult = admin.removeTopicsFromMirror(mirrorName, Set.of(topicName), new RemoveTopicsFromMirrorOptions());
                    deleteMirrorTopicResult.all().get();
                    System.out.println("Delete mirror topic topic " + topicName + ".");
                }
            } else {
                System.out.println("error when delete mirror topic " + topicName + ".");
            }

        }

        public void addTopicToMirror(MirrorCommandOptions opts) throws Exception {
            String topicName = opts.topic().get();
            String mirrorName = opts.mirror().get();

            System.out.println("Adding topic " + topicName + " to mirror " + mirrorName);

            // Find coordinator for the mirror
            FindCoordinatorResult findCoordinatorResult = adminClient.findCoordinator(mirrorName);
            Optional<Node> coordinator = Optional.ofNullable(findCoordinatorResult.node().get());

            if (coordinator.isEmpty()) {
                throw new RuntimeException("Could not find coordinator for mirror " + mirrorName);
            }

            System.out.println("Found coordinator " + coordinator.get().idString() + " for mirror " + mirrorName);

            // Create the mirror topic with the provided topic ID
            NewTopic newTopic = new NewTopic(topicName,
                Optional.empty(), // partitions - will use source topic partitions
                opts.replicationFactor(), // replicationFactor - use provided value or cluster default
                Optional.of(opts.remoteBootstrapServer().get()),
                Optional.of(opts.topicId().get()),
                Optional.of(mirrorName));

            // Create topic using coordinator's bootstrap server
            Node node = coordinator.get();
            String bootstrapServer = node.host() + ":" + node.port();
            System.out.println("Creating mirror topic " + topicName + " using bootstrap server " + bootstrapServer);

            try (Admin admin = createAdminClient(Optional.of(bootstrapServer), commandConfig)) {
                // try to create the topic, if failed, we attach the mirror to an existing topic
                CreateTopicsResult createResult = admin.createTopics(Set.of(newTopic),
                    new CreateTopicsOptions().retryOnQuotaViolation(false));
                createResult.all().whenComplete((future, ex) -> {
                    System.out.println("Create mirror topic result: " + ex);

                    if (ex != null) {
                       if (ex instanceof TopicExistsException) {
                           System.out.println("Mirror topic " + topicName + " already exists, attaching mirror to it.");
                           try (Admin admin1 = createAdminClient(Optional.of(bootstrapServer), commandConfig)) {
                               AddTopicsToMirrorResult attachMirrorTopicResult = admin1.addTopicsToMirror(Collections.singletonMap(topicName, mirrorName), new AddTopicsToMirrorOptions());
                               try {
                                   attachMirrorTopicResult.all().get();
                                   System.out.println("Successfully attached topic " + topicName + " to mirror " + mirrorName);
                               } catch (Exception e) {
                                   throw new RuntimeException(e);
                               }
                           }
                       } else {
                           throw new RuntimeException("Failed to create topic " + topicName, ex);
                       }
                    } else {
                        System.out.println("Successfully added topic " + topicName + " to mirror " + mirrorName);
                    }
                });
            }
        }

        @Override
        public void close() throws Exception {
            adminClient.close();
        }
    }

    public static final class MirrorCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        private final ArgumentAcceptingOptionSpec<String> remoteBootstrapServerOpt;
        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorConfigOpt;
        private final OptionSpecBuilder createOpt;
        private final OptionSpecBuilder addOpt;
        private final OptionSpecBuilder removeOpt;
        private final ArgumentAcceptingOptionSpec<String> mirrorOpt;


        private final ArgumentAcceptingOptionSpec<String> topicOpt;
        private final ArgumentAcceptingOptionSpec<String> topicIdOpt;
        private final ArgumentAcceptingOptionSpec<Short> replicationFactorOpt;

        public MirrorCommandOptions(String[] args) {
            super(args);

            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The destination Kafka server to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);

            remoteBootstrapServerOpt = parser.accepts("remote-bootstrap-server", "The source Kafka server to connect to for mirroring.")
                .withRequiredArg()
                .describedAs("remote server to connect to")
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
            addOpt = parser.accepts("add", "Add a topic to an existing cluster mirror.");
            removeOpt = parser.accepts("remove", "remove a topic from an existing cluster mirror.");

            mirrorOpt = parser.accepts("mirror", "The name of the cluster mirror.")
                .withRequiredArg()
                .describedAs("mirror")
                .ofType(String.class);

            topicOpt = parser.accepts("topic", "The topic name to add to the mirror.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);

            topicIdOpt = parser.accepts("topic-id", "The topic ID to use when adding the topic to the mirror.")
                .withRequiredArg()
                .describedAs("topic-id")
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

        public Optional<String> remoteBootstrapServer() {
            return valueAsOption(remoteBootstrapServerOpt);
        }

        public Properties commandConfig() throws IOException {
            if (has(commandConfigOpt)) {
                return Utils.loadProps(options.valueOf(commandConfigOpt));
            } else {
                return new Properties();
            }
        }

        public Map<String, String> mirrorConfig() throws IOException {
            Map<String, String> config = new HashMap<>();
            if (has(mirrorConfigOpt)) {
                Properties properties = Utils.loadProps(options.valueOf(mirrorConfigOpt));
                properties.forEach((k, v) -> config.put(k.toString(), v.toString()));
            }
            return config;
        }

        public Optional<String> mirror() {
            return valueAsOption(mirrorOpt);
        }

        public Optional<String> topic() {
            return valueAsOption(topicOpt);
        }

        public Optional<String> topicId() {
            return valueAsOption(topicIdOpt);
        }

        public Optional<Short> replicationFactor() {
            return valueAsOption(replicationFactorOpt);
        }

        @SuppressWarnings("NPathComplexity")
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

            if (has(createOpt) && !has(mirrorConfigOpt) && !has(remoteBootstrapServerOpt))
                throw new IllegalArgumentException("Either --mirror-config or --remote-bootstrap-server must be specified when creating a mirror");

            if (has(addOpt) && !has(topicOpt))
                throw new IllegalArgumentException("--topic must be specified when adding a topic to a mirror");

            if (has(addOpt) && !has(remoteBootstrapServerOpt))
                throw new IllegalArgumentException("--remote-bootstrap-server must be specified when adding a topic to a mirror");

            if (has(addOpt) && !has(topicIdOpt))
                throw new IllegalArgumentException("--topic-id must be specified when adding a topic to a mirror");
        }
    }
}
