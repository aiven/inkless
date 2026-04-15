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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.RemoveVirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.VirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.VirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.VirtualClusterUserRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Mutable changes for a single virtual cluster while applying a metadata delta.
 */
public final class VirtualClusterDelta {
    private String name;
    private final Map<String, TopicLinkImage> topicLinks = new HashMap<>();
    private final Set<String> users = new HashSet<>();
    private final Set<String> groups = new HashSet<>();

    public VirtualClusterDelta(String name) {
        this.name = name;
    }

    public VirtualClusterDelta(String name, VirtualClusterImage previous) {
        this.name = name;
        topicLinks.putAll(previous.topicLinks());
        users.addAll(previous.users());
        groups.addAll(previous.groups());
    }

    public String name() {
        return name;
    }

    public void replay(VirtualClusterTopicLinkRecord record) {
        topicLinks.put(record.linkName(), new TopicLinkImage(record.linkName(), record.physicalTopicName()));
    }

    public void replay(RemoveVirtualClusterTopicLinkRecord record) {
        topicLinks.remove(record.linkName());
    }

    public void replay(VirtualClusterUserRecord record) {
        users.add(record.userName());
    }

    public void replay(org.apache.kafka.common.metadata.RemoveVirtualClusterUserRecord record) {
        users.remove(record.userName());
    }

    public void replay(VirtualClusterGroupRecord record) {
        groups.add(record.groupId());
    }

    public void replay(org.apache.kafka.common.metadata.RemoveVirtualClusterGroupRecord record) {
        groups.remove(record.groupId());
    }

    boolean hasUser(String userName) {
        return users.contains(userName);
    }

    boolean hasGroup(String groupId) {
        return groups.contains(groupId);
    }

    public VirtualClusterImage apply() {
        return new VirtualClusterImage(name, Map.copyOf(topicLinks), Set.copyOf(users), Set.copyOf(groups));
    }

    @Override
    public String toString() {
        return "VirtualClusterDelta(name=" + name + ", topicLinks=" + topicLinks +
            ", users=" + users + ", groups=" + groups + ")";
    }
}
