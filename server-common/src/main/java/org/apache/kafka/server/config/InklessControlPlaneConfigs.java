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
package org.apache.kafka.server.config;

import java.util.Set;

/**
 * Property names for the Inkless control plane connection strings the management plane may
 * repoint at runtime.
 *
 * `InklessConfig` and the Postgres control-plane config classes that assemble these same names
 * live in `:storage:inkless`, which depends on `:server`. Defining the names here in
 * `:server-common`, a module both `:server` and `:storage:inkless` depend on, lets `:server`'s
 * dynamic-config registry and `:core`'s `DynamicInklessControlPlaneConfig` agree on the exact
 * property names without a circular module dependency.
 */
public final class InklessControlPlaneConfigs {
    private static final String PREFIX = "inkless.control.plane.";

    public static final String CONNECTION_STRING_CONFIG = PREFIX + "connection.string";
    public static final String READ_CONNECTION_STRING_CONFIG = PREFIX + "read.connection.string";
    public static final String WRITE_CONNECTION_STRING_CONFIG = PREFIX + "write.connection.string";

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            CONNECTION_STRING_CONFIG,
            READ_CONNECTION_STRING_CONFIG,
            WRITE_CONNECTION_STRING_CONFIG);

    private InklessControlPlaneConfigs() {
    }
}
