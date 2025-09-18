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
package kafka.server.coordinator;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

public record ClusterLinkPartitionKey(String clusterLinkId, Uuid topicId) {

    public ClusterLinkPartitionKey(String clusterLinkId, Uuid topicId) {
        this.clusterLinkId = Objects.requireNonNull(clusterLinkId, "clusterLinkId cannot be null");
        this.topicId = Objects.requireNonNull(topicId, "topicId cannot be null");
    }

    /**
     * Returns a ClusterLinkPartitionKey from input string of format - clusterLinkId:topicId
     */
    public static ClusterLinkPartitionKey getInstance(String key) {
        validate(key);
        String[] tokens = key.split(":");
        return new ClusterLinkPartitionKey(
            tokens[0].trim(),
            Uuid.fromString(tokens[1])
        );
    }

    public static ClusterLinkPartitionKey getInstance(String groupId, Uuid topicId) {
        return new ClusterLinkPartitionKey(groupId, topicId);
    }

    public String asCoordinatorKey() {
        return asCoordinatorKey(clusterLinkId, topicId);
    }

    public static String asCoordinatorKey(String clusterLinkId, Uuid topicId) {
        return String.format("%s:%s", clusterLinkId, topicId);
    }

    public static void validate(String key) {
        Objects.requireNonNull(key, "Cluster link key cannot be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Cluster link key cannot be empty");
        }

        String[] tokens = key.split(":");
        if (tokens.length != 3) {
            throw new IllegalArgumentException("Invalid key format: expected - clusterLinkId:topicId, found -  " + key);
        }

        if (tokens[0].trim().isEmpty()) {
            throw new IllegalArgumentException("clusterLinkId must be alphanumeric string");
        }

        try {
            Uuid.fromString(tokens[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid topic ID: " + tokens[1], e);
        }
    }

    @Override
    public String toString() {
        return "ClusterLinkPartitionKey{" +
                "clusterLinkId='" + clusterLinkId + '\'' +
                ", topicId=" + topicId +
                '}';
    }
}
