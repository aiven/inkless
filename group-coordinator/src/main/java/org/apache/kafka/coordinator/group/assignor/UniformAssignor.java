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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

/**
 * The Uniform Assignor distributes topic partitions among group members for a
 * balanced and potentially rack aware assignment.
 * The assignor employs two different strategies based on the nature of topic
 * subscriptions across the group members:
 * <ul>
 *     <li>
 *         <b> Uniform Homogeneous Assignment Builder: </b> This strategy is used when all members have subscribed
 *         to the same set of topics.
 *     </li>
 *     <li>
 *         <b> Uniform Heterogeneous Assignment Builder: </b> This strategy is used when members have varied topic
 *         subscriptions.
 *     </li>
 * </ul>
 *
 * The appropriate strategy is automatically chosen based on the current members' topic subscriptions.
 *
 * @see UniformHomogeneousAssignmentBuilder
 * @see UniformHeterogeneousAssignmentBuilder
 */
public class UniformAssignor implements ConsumerGroupPartitionAssignor {
    private static final Logger LOG = LoggerFactory.getLogger(UniformAssignor.class);
    public static final String NAME = "uniform";

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Perform the group assignment given the current members and
     * topics metadata.
     *
     * @param groupSpec                     The assignment specification that included member metadata.
     * @param subscribedTopicDescriber      The topic and cluster metadata describer {@link SubscribedTopicDescriber}.
     * @return The new target assignment for the group.
     */
    @Override
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        if (groupSpec.memberIds().isEmpty())
            return new GroupAssignment(Map.of());

        if (groupSpec.subscriptionType().equals(HOMOGENEOUS)) {
            LOG.debug("Detected that all members are subscribed to the same set of topics, invoking the "
                + "homogeneous assignment algorithm");
            return new UniformHomogeneousAssignmentBuilder(groupSpec, subscribedTopicDescriber)
                .build();
        } else {
            LOG.debug("Detected that the members are subscribed to different sets of topics, invoking the "
                + "heterogeneous assignment algorithm");
            return new UniformHeterogeneousAssignmentBuilder(groupSpec, subscribedTopicDescriber)
                .build();
        }
    }
}
