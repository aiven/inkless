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

import org.apache.kafka.common.metadata.RemoveVirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterUserRecord;
import org.apache.kafka.common.metadata.VirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.common.metadata.VirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.VirtualClusterUserRecord;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class VirtualClustersDelta {
    private final VirtualClustersImage image;
    private final Map<String, VirtualClusterDelta> changedClusters = new HashMap<>();
    private final Set<String> deletedClusters = new HashSet<>();

    public VirtualClustersDelta(VirtualClustersImage image) {
        this.image = image;
    }

    public VirtualClustersImage image() {
        return image;
    }

    public void finishSnapshot() {
        for (String name : image.clusters().keySet()) {
            if (!changedClusters.containsKey(name)) {
                deletedClusters.add(name);
            }
        }
    }

    public void handleMetadataVersionChange(MetadataVersion newVersion) {
        if (!newVersion.isVirtualClusterSupported()) {
            changedClusters.clear();
            deletedClusters.clear();
            deletedClusters.addAll(image.clusters().keySet());
        }
    }

    public void replay(VirtualClusterRecord record) {
        String name = record.name();
        deletedClusters.remove(name);
        changedClusters.put(name, new VirtualClusterDelta(name));
    }

    public void replay(RemoveVirtualClusterRecord record) {
        String name = record.name();
        deletedClusters.add(name);
        changedClusters.remove(name);
    }

    public void replay(VirtualClusterTopicLinkRecord record) {
        getOrCreateClusterDelta(record.virtualClusterName()).replay(record);
    }

    public void replay(RemoveVirtualClusterTopicLinkRecord record) {
        getOrCreateClusterDelta(record.virtualClusterName()).replay(record);
    }

    public void replay(VirtualClusterUserRecord record) {
        getOrCreateClusterDelta(record.virtualClusterName()).replay(record);
    }

    public void replay(RemoveVirtualClusterUserRecord record) {
        VirtualClusterDelta delta = getOrCreateClusterDeltaForUserRemoval(record.userName());
        delta.replay(record);
    }

    public void replay(VirtualClusterGroupRecord record) {
        getOrCreateClusterDelta(record.virtualClusterName()).replay(record);
    }

    public void replay(RemoveVirtualClusterGroupRecord record) {
        VirtualClusterDelta delta = getOrCreateClusterDeltaForGroupRemoval(record.groupId());
        delta.replay(record);
    }

    private VirtualClusterDelta getOrCreateClusterDelta(String virtualClusterName) {
        if (deletedClusters.contains(virtualClusterName)) {
            throw new RuntimeException("Cannot modify deleted virtual cluster " + virtualClusterName);
        }
        return changedClusters.computeIfAbsent(virtualClusterName, n -> {
            VirtualClusterImage previous = image.virtualCluster(n).orElseThrow(() ->
                new RuntimeException("Virtual cluster " + n + " not found"));
            return new VirtualClusterDelta(n, previous);
        });
    }

    private VirtualClusterDelta getOrCreateClusterDeltaForUserRemoval(String userName) {
        String vcName = findVirtualClusterForUser(userName).orElseThrow(() ->
            new RuntimeException("User " + userName + " is not assigned to a virtual cluster"));
        if (deletedClusters.contains(vcName)) {
            throw new RuntimeException("Virtual cluster " + vcName + " is deleted");
        }
        return changedClusters.computeIfAbsent(vcName, n -> {
            VirtualClusterImage previous = image.virtualCluster(n).orElseThrow();
            return new VirtualClusterDelta(n, previous);
        });
    }

    private Optional<String> findVirtualClusterForUser(String userName) {
        Optional<String> fromImage = image.virtualClusterNameForUser(userName);
        if (fromImage.isPresent()) {
            return fromImage;
        }
        for (Map.Entry<String, VirtualClusterDelta> entry : changedClusters.entrySet()) {
            if (entry.getValue().hasUser(userName)) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }

    private VirtualClusterDelta getOrCreateClusterDeltaForGroupRemoval(String groupId) {
        String vcName = findVirtualClusterForGroup(groupId).orElseThrow(() ->
            new RuntimeException("Group " + groupId + " is not assigned to a virtual cluster"));
        if (deletedClusters.contains(vcName)) {
            throw new RuntimeException("Virtual cluster " + vcName + " is deleted");
        }
        return changedClusters.computeIfAbsent(vcName, n -> {
            VirtualClusterImage previous = image.virtualCluster(n).orElseThrow();
            return new VirtualClusterDelta(n, previous);
        });
    }

    private Optional<String> findVirtualClusterForGroup(String groupId) {
        Optional<String> fromImage = image.virtualClusterNameForGroup(groupId);
        if (fromImage.isPresent()) {
            return fromImage;
        }
        for (Map.Entry<String, VirtualClusterDelta> entry : changedClusters.entrySet()) {
            if (entry.getValue().hasGroup(groupId)) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }

    public VirtualClustersImage apply() {
        Map<String, VirtualClusterImage> newClusters = new HashMap<>(image.clusters());
        for (String name : deletedClusters) {
            newClusters.remove(name);
        }
        for (Map.Entry<String, VirtualClusterDelta> entry : changedClusters.entrySet()) {
            newClusters.put(entry.getKey(), entry.getValue().apply());
        }
        return new VirtualClustersImage(Map.copyOf(newClusters));
    }

    @Override
    public String toString() {
        return "VirtualClustersDelta(" +
            "changedClusters=" + changedClusters +
            ", deletedClusters=" + deletedClusters +
            ')';
    }
}
