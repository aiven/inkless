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

import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.VirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.common.metadata.VirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.VirtualClusterUserRecord;
import org.apache.kafka.image.node.VirtualClustersImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * All virtual clusters in the metadata image (KIP-1134).
 */
public record VirtualClustersImage(Map<String, VirtualClusterImage> clusters) {
    public static final VirtualClustersImage EMPTY = new VirtualClustersImage(Map.of());

    public VirtualClustersImage {
        clusters = Collections.unmodifiableMap(clusters);
    }

    public boolean isEmpty() {
        return clusters.isEmpty();
    }

    public Optional<VirtualClusterImage> virtualCluster(String name) {
        return Optional.ofNullable(clusters.get(name));
    }

    /**
     * Returns the virtual cluster that currently owns this user principal, if any.
     */
    public Optional<String> virtualClusterNameForUser(String userName) {
        for (VirtualClusterImage vc : clusters.values()) {
            if (vc.users().contains(userName)) {
                return Optional.of(vc.name());
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the virtual cluster that currently owns this consumer group id, if any.
     */
    public Optional<String> virtualClusterNameForGroup(String groupId) {
        for (VirtualClusterImage vc : clusters.values()) {
            if (vc.groups().contains(groupId)) {
                return Optional.of(vc.name());
            }
        }
        return Optional.empty();
    }

    public void write(ImageWriter writer) {
        List<String> sortedNames = new ArrayList<>(clusters.keySet());
        Collections.sort(sortedNames);
        short vcRecVersion = MetadataRecordType.VIRTUAL_CLUSTER_RECORD.highestSupportedVersion();
        short linkRecVersion = MetadataRecordType.VIRTUAL_CLUSTER_TOPIC_LINK_RECORD.highestSupportedVersion();
        short userRecVersion = MetadataRecordType.VIRTUAL_CLUSTER_USER_RECORD.highestSupportedVersion();
        short groupRecVersion = MetadataRecordType.VIRTUAL_CLUSTER_GROUP_RECORD.highestSupportedVersion();

        for (String name : sortedNames) {
            VirtualClusterImage vc = clusters.get(name);
            writer.write(new ApiMessageAndVersion(new VirtualClusterRecord().setName(vc.name()), vcRecVersion));
            TreeMap<String, TopicLinkImage> sortedLinks = new TreeMap<>(vc.topicLinks());
            for (TopicLinkImage link : sortedLinks.values()) {
                writer.write(new ApiMessageAndVersion(
                    new VirtualClusterTopicLinkRecord()
                        .setVirtualClusterName(vc.name())
                        .setLinkName(link.linkName())
                        .setPhysicalTopicName(link.physicalTopicName()),
                    linkRecVersion));
            }
            ArrayList<String> sortedUsers = new ArrayList<>(vc.users());
            Collections.sort(sortedUsers);
            for (String user : sortedUsers) {
                writer.write(new ApiMessageAndVersion(
                    new VirtualClusterUserRecord()
                        .setVirtualClusterName(vc.name())
                        .setUserName(user),
                    userRecVersion));
            }
            ArrayList<String> sortedGroups = new ArrayList<>(vc.groups());
            Collections.sort(sortedGroups);
            for (String group : sortedGroups) {
                writer.write(new ApiMessageAndVersion(
                    new VirtualClusterGroupRecord()
                        .setVirtualClusterName(vc.name())
                        .setGroupId(group),
                    groupRecVersion));
            }
        }
    }

    @Override
    public String toString() {
        return new VirtualClustersImageNode(this).stringify();
    }
}
