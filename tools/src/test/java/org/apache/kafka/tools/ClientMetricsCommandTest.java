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
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListConfigResourcesResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Exit;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientMetricsCommandTest {
    private final String bootstrapServer = "localhost:9092";
    private final String clientMetricsName = "cm";

    @Test
    public void testOptionsNoActionFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer});
    }

    @Test
    public void testOptionsListSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--list"});
        assertTrue(opts.hasListOption());
    }

    @Test
    public void testOptionsDescribeNoNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--describe"});
        assertTrue(opts.hasDescribeOption());
    }

    @Test
    public void testOptionsDescribeWithNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--name", clientMetricsName});
        assertTrue(opts.hasDescribeOption());
    }

    @Test
    public void testOptionsDeleteNoNameFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--delete"});
    }

    @Test
    public void testOptionsDeleteWithNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--delete", "--name", clientMetricsName});
        assertTrue(opts.hasDeleteOption());
    }

    @Test
    public void testOptionsAlterNoNameFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--alter"});
    }

    @Test
    public void testOptionsAlterGenerateNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--alter", "--generate-name"});
        assertTrue(opts.hasAlterOption());
    }

    @Test
    public void testOptionsAlterWithNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--alter", "--name", clientMetricsName});
        assertTrue(opts.hasAlterOption());
    }

    @Test
    public void testOptionsAlterAllOptionsSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--alter", "--name", clientMetricsName,
                "--interval", "1000", "--match", "client_id=abc", "--metrics", "org.apache.kafka."});
        assertTrue(opts.hasAlterOption());

    }

    @Test
    public void testOptionsAlterInvalidInterval() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[]{"--bootstrap-server", bootstrapServer, "--alter", "--name", clientMetricsName,
                "--interval", "abc"}));
        assertEquals("Invalid interval value. Enter an integer, or leave empty to reset.", exception.getMessage());
    }

    @Test
    public void testAlter() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        AlterConfigsResult result = AdminClientTestUtils.alterConfigsResult(new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName));
        when(adminClient.incrementalAlterConfigs(any(), any())).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.alterClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--alter",
                                     "--name", clientMetricsName, "--metrics", "org.apache.kafka.producer.",
                                     "--interval", "5000", "--match", "client_id=CLIENT1"}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Altered client metrics config for " + clientMetricsName + "."));
    }

    @Test
    public void testAlterGenerateName() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        AlterConfigsResult result = AdminClientTestUtils.alterConfigsResult(new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "whatever"));
        when(adminClient.incrementalAlterConfigs(any(), any())).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.alterClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--alter",
                                     "--generate-name", "--metrics", "org.apache.kafka.producer.",
                                     "--interval", "5000", "--match", "client_id=CLIENT1"}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Altered client metrics config"));
    }

    @Test
    public void testAlterResetConfigs() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        AlterConfigsResult result = AdminClientTestUtils.alterConfigsResult(new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName));
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> configCaptor = ArgumentCaptor.forClass(Map.class);
        when(adminClient.incrementalAlterConfigs(configCaptor.capture(), any())).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.alterClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--alter",
                                     "--name", clientMetricsName, "--metrics", "",
                                     "--interval", "", "--match", ""}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        Map<ConfigResource, Collection<AlterConfigOp>> alteredConfigOps = configCaptor.getValue();
        assertNotNull(alteredConfigOps, "alteredConfigOps should not be null");
        assertEquals(1, alteredConfigOps.size(), "Should have exactly one ConfigResource");
        assertEquals(3, alteredConfigOps.values().iterator().next().size(), "Should have exactly 3 operations");
        for (Collection<AlterConfigOp> operations : alteredConfigOps.values()) {
            for (AlterConfigOp op : operations) {
                assertEquals(AlterConfigOp.OpType.DELETE, op.opType(),
                        "Expected DELETE operation for config: " + op.configEntry().name());
            }
        }
        assertTrue(capturedOutput.contains("Altered client metrics config for " + clientMetricsName + "."));
    }

    @Test
    public void testDelete() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ConfigResource cr = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName);
        Config cfg = new Config(Collections.singleton(new ConfigEntry("metrics", "org.apache.kafka.producer.")));
        DescribeConfigsResult describeResult = AdminClientTestUtils.describeConfigsResult(cr, cfg);
        when(adminClient.describeConfigs(any())).thenReturn(describeResult);
        AlterConfigsResult alterResult = AdminClientTestUtils.alterConfigsResult(cr);
        when(adminClient.incrementalAlterConfigs(any(), any())).thenReturn(alterResult);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.deleteClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--delete",
                                     "--name", clientMetricsName}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Deleted client metrics config for " + clientMetricsName + "."));
    }

    @Test
    public void testDescribe() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ConfigResource cr = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName);
        ListConfigResourcesResult listConfigResourcesResult = AdminClientTestUtils.listConfigResourcesResult(Map.of(
            ConfigResource.Type.CLIENT_METRICS, Set.of(clientMetricsName)
        ));
        when(adminClient.listConfigResources(any(), any())).thenReturn(listConfigResourcesResult);
        Config cfg = new Config(Collections.singleton(new ConfigEntry("metrics", "org.apache.kafka.producer.")));
        DescribeConfigsResult describeResult = AdminClientTestUtils.describeConfigsResult(cr, cfg);
        when(adminClient.describeConfigs(any())).thenReturn(describeResult);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe",
                                     "--name", clientMetricsName}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Client metrics configs for " + clientMetricsName + " are:"));
        assertTrue(capturedOutput.contains("metrics=org.apache.kafka.producer."));
    }

    @Test
    public void testDescribeNonExistentClientMetric() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ListConfigResourcesResult listConfigResourcesResult = AdminClientTestUtils.listConfigResourcesResult(Map.of(
            ConfigResource.Type.CLIENT_METRICS, Set.of()
        ));
        when(adminClient.listConfigResources(any(), any())).thenReturn(listConfigResourcesResult);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                    new String[]{"--bootstrap-server", bootstrapServer, "--describe",
                        "--name", clientMetricsName}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("The client metric resource " + clientMetricsName + " doesn't exist and doesn't have dynamic config."));
    }

    @Test
    public void testDescribeAll() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ListConfigResourcesResult result = AdminClientTestUtils.listConfigResourcesResult(clientMetricsName);
        when(adminClient.listConfigResources(any(), any())).thenReturn(result);
        ConfigResource cr = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName);
        Config cfg = new Config(Collections.singleton(new ConfigEntry("metrics", "org.apache.kafka.producer.")));
        DescribeConfigsResult describeResult = AdminClientTestUtils.describeConfigsResult(cr, cfg);
        when(adminClient.describeConfigs(any())).thenReturn(describeResult);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe"}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Client metrics configs for " + clientMetricsName + " are:"));
        assertTrue(capturedOutput.contains("metrics=org.apache.kafka.producer."));
    }

    @Test
    public void testList() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ListConfigResourcesResult result = AdminClientTestUtils.listConfigResourcesResult("one", "two");
        when(adminClient.listConfigResources(any(), any())).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listClientMetrics();
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("one,two", String.join(",", capturedOutput.split("\n")));
    }

    @Test
    public void testListFailsWithUnsupportedVersionException() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ListConfigResourcesResult result = AdminClientTestUtils.listConfigResourcesResult(Errors.UNSUPPORTED_VERSION.exception());
        when(adminClient.listConfigResources(any(), any())).thenReturn(result);

        assertThrows(ExecutionException.class, () -> service.listClientMetrics());
    }

    private void assertInitializeInvalidOptionsExitCode(int expected, String[] options) {
        Exit.setExitProcedure((exitCode, message) -> {
            assertEquals(expected, exitCode);
            throw new RuntimeException();
        });
        try {
            assertThrows(RuntimeException.class, () -> new ClientMetricsCommand.ClientMetricsCommandOptions(options));
        } finally {
            Exit.resetExitProcedure();
        }
    }
}
