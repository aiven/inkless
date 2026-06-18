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

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;

import java.util.List;
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
            case "seal":
            case "repair":
                throw new RuntimeException("Command \"" + command + "\" not implemented");
            default:
                throw new RuntimeException("Unknown command " + command);
        }
    }

    private static ArgumentParser argumentParser() {
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

        for (Subparser subparser : List.of(stateParser, sealParser, repairParser)) {
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
                    .help("Topic name for the specified topic.");
        }

        sealParser.addArgument("--dry-run", "-d")
                .action(storeTrue())
                .help("Whether to only perform validation when adjusting the seal offset.");

        return parser;
    }
}
