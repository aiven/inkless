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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A listing of a cluster mirror.
 */
public class ClusterMirrorListing {
    private final String mirrorName;
    private final String sourceBootstrap;
    private final String sourceClusterId;
    private final int topicCount;
    private final List<String> topics;

    /**
     * Create an instance with the specified parameters.
     *
     * @param mirrorName Mirror name
     * @param sourceBootstrap Source cluster bootstrap servers
     * @param sourceClusterId Source cluster ID
     * @param topicCount Number of topics configured for this mirror
     * @param topics List of topic names configured for this mirror
     */
    public ClusterMirrorListing(String mirrorName, String sourceBootstrap, String sourceClusterId, int topicCount, List<String> topics) {
        this.mirrorName = mirrorName;
        this.sourceBootstrap = sourceBootstrap;
        this.sourceClusterId = sourceClusterId;
        this.topicCount = topicCount;
        this.topics = topics;
    }

    /**
     * Create an instance with the specified parameters.
     *
     * @param mirrorName Mirror name
     * @param sourceBootstrap Source cluster bootstrap servers
     * @param sourceClusterId Source cluster ID
     * @param topicCount Number of topics configured for this mirror
     */
    public ClusterMirrorListing(String mirrorName, String sourceBootstrap, String sourceClusterId, int topicCount) {
        this(mirrorName, sourceBootstrap, sourceClusterId, topicCount, Collections.emptyList());
    }

    /**
     * Create an instance with the specified parameters (backwards compatibility).
     *
     * @param mirrorName Mirror name
     * @param sourceBootstrap Source cluster bootstrap servers
     * @param topicCount Number of topics configured for this mirror
     */
    public ClusterMirrorListing(String mirrorName, String sourceBootstrap, int topicCount) {
        this(mirrorName, sourceBootstrap, "", topicCount, Collections.emptyList());
    }

    /**
     * Create an instance with the specified parameters (backwards compatibility).
     *
     * @param mirrorName Mirror name
     * @param sourceBootstrap Source cluster bootstrap servers
     */
    public ClusterMirrorListing(String mirrorName, String sourceBootstrap) {
        this(mirrorName, sourceBootstrap, "", 0, Collections.emptyList());
    }

    /**
     * The mirror name.
     *
     * @return Mirror name
     */
    public String mirrorName() {
        return mirrorName;
    }

    /**
     * The source cluster bootstrap servers.
     *
     * @return Source bootstrap servers, or null if not available
     */
    public String sourceBootstrap() {
        return sourceBootstrap;
    }

    /**
     * The source cluster ID.
     *
     * @return Source cluster ID, or empty string if not yet resolved
     */
    public String sourceClusterId() {
        return sourceClusterId;
    }

    /**
     * The number of topics configured for this mirror.
     *
     * @return Number of topics, or 0 if mirror has no topics configured
     */
    public int topicCount() {
        return topicCount;
    }

    /**
     * The topic names configured for this mirror.
     *
     * @return List of topic names, or empty list if not requested or not available
     */
    public List<String> topics() {
        return topics;
    }

    @Override
    public String toString() {
        return "ClusterMirrorListing(mirrorName='" + mirrorName + "', sourceBootstrap='" + sourceBootstrap
                + "', sourceClusterId='" + sourceClusterId + "', topicCount=" + topicCount
                + ", topics=" + topics + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(mirrorName, sourceBootstrap, sourceClusterId, topicCount, topics);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterMirrorListing)) return false;
        ClusterMirrorListing that = (ClusterMirrorListing) o;
        return topicCount == that.topicCount &&
               Objects.equals(mirrorName, that.mirrorName) &&
               Objects.equals(sourceBootstrap, that.sourceBootstrap) &&
               Objects.equals(sourceClusterId, that.sourceClusterId) &&
               Objects.equals(topics, that.topics);
    }
}
