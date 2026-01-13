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
package org.apache.kafka.server.policy;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.server.policy.AlterConfigV2Policy.RequestMetadata;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class AlterConfigV2PolicyTest {

    @Test
    public void testRequestMetadataEquals() {
        RequestMetadata requestMetadata = new RequestMetadata(
            new ConfigResource(Type.BROKER, "0"),
            Map.of("foo", "bar"),
            Map.of("foo", "baz")
        );

        assertEquals(requestMetadata, requestMetadata);

        assertNotEquals(requestMetadata, null);
        assertNotEquals(requestMetadata, new Object());
        assertNotEquals(requestMetadata, new RequestMetadata(
            new ConfigResource(Type.BROKER, "1"),
            Map.of("foo", "bar"),
            Map.of("foo", "baz")
        ));
        assertNotEquals(requestMetadata, new RequestMetadata(
            new ConfigResource(Type.BROKER, "0"),
            Map.of("foo", "bar"),
            Map.of()
        ));
        assertNotEquals(requestMetadata, new RequestMetadata(
            new ConfigResource(Type.BROKER, "0"),
            Map.of(),
            Map.of("foo", "baz")
        ));
    }
}
