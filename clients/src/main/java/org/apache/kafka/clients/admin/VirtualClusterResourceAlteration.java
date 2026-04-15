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

package org.apache.kafka.clients.admin;

import java.util.Objects;

/**
 * Alteration of a user, consumer group, or topic link on a virtual cluster (KIP-1134).
 * Resource types: 0=user, 1=topic link, 2=consumer group. Operations: 0=add, 1=remove.
 * For topic link add, set {@code physicalTopicName} to the physical topic name.
 */
public class VirtualClusterResourceAlteration {
    private final byte resourceType;
    private final byte resourceOperation;
    private final String resourceName;
    private final String physicalTopicName;

    public VirtualClusterResourceAlteration(
        byte resourceType,
        byte resourceOperation,
        String resourceName,
        String physicalTopicName
    ) {
        this.resourceType = resourceType;
        this.resourceOperation = resourceOperation;
        this.resourceName = Objects.requireNonNull(resourceName);
        this.physicalTopicName = physicalTopicName;
    }

    public byte resourceType() {
        return resourceType;
    }

    public byte resourceOperation() {
        return resourceOperation;
    }

    public String resourceName() {
        return resourceName;
    }

    public String physicalTopicName() {
        return physicalTopicName;
    }
}
