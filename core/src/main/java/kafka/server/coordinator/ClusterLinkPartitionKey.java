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

import java.util.Objects;

public record ClusterLinkPartitionKey(String clusterLinkId) {

    public ClusterLinkPartitionKey(String clusterLinkId) {
        this.clusterLinkId = Objects.requireNonNull(clusterLinkId, "clusterLinkId cannot be null");
    }

    /**
     * Returns a ClusterLinkPartitionKey from input string of format - clusterLinkId:topicId
     */
    public static ClusterLinkPartitionKey getInstance(String key) {
        validate(key);
        return new ClusterLinkPartitionKey(key);
    }

    public String asCoordinatorKey() {
        return asCoordinatorKey(clusterLinkId);
    }

    public static String asCoordinatorKey(String clusterLinkId) {
        return clusterLinkId;
    }

    public static void validate(String key) {
        Objects.requireNonNull(key, "Cluster link key cannot be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Cluster link key cannot be empty");
        }
    }
}
