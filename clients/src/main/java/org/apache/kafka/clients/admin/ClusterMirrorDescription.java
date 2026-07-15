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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A detailed description of a cluster mirror.
 */
public class ClusterMirrorDescription {
    private final String mirrorName;
    private final Map<String, Set<LeaderStateDescription>> topics;
    private final Set<AclOperation> authorizedOperations;

    public ClusterMirrorDescription(String mirrorName,
                                    Map<String, Set<LeaderStateDescription>> topics,
                                    Set<AclOperation> authorizedOperations) {
        this.mirrorName = mirrorName;
        this.topics = Collections.unmodifiableMap(topics);
        this.authorizedOperations = authorizedOperations;
    }

    public String mirrorName() {
        return mirrorName;
    }

    public Map<String, Set<LeaderStateDescription>> topics() {
        return topics;
    }

    public Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterMirrorDescription that = (ClusterMirrorDescription) o;
        return Objects.equals(mirrorName, that.mirrorName) &&
               Objects.equals(topics, that.topics) &&
               Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mirrorName, topics, authorizedOperations);
    }

    @Override
    public String toString() {
        return "ClusterMirrorDescription{" +
               "mirrorName='" + mirrorName + '\'' +
               ", topics=" + topics +
               ", authorizedOperations=" + authorizedOperations +
               '}';
    }

    /** Represents the mirroring state of a leader partition. */
    public static class LeaderStateDescription {
        private final TopicPartition topicPartition;
        private final long sourceOffset;
        private final long destinationOffset;
        private final long lag;
        private final String state;
        private final int retryAttempt;
        private final String errorMessage;

        public LeaderStateDescription(TopicPartition topicPartition, long sourceOffset, long destinationOffset,
                                      long lag, String state, int retryAttempt, String errorMessage) {
            this.topicPartition = topicPartition;
            this.sourceOffset = sourceOffset;
            this.destinationOffset = destinationOffset;
            this.lag = lag;
            this.state = state;
            this.retryAttempt = retryAttempt;
            this.errorMessage = errorMessage;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        public long sourceOffset() {
            return sourceOffset;
        }

        public long destinationOffset() {
            return destinationOffset;
        }

        public long lag() {
            return lag;
        }

        public String state() {
            return state;
        }

        public int retryAttempt() {
            return retryAttempt;
        }

        public String errorMessage() {
            return errorMessage;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LeaderStateDescription that = (LeaderStateDescription) o;
            return sourceOffset == that.sourceOffset &&
                   destinationOffset == that.destinationOffset &&
                   lag == that.lag &&
                   retryAttempt == that.retryAttempt &&
                   Objects.equals(topicPartition, that.topicPartition) &&
                   Objects.equals(state, that.state) &&
                   Objects.equals(errorMessage, that.errorMessage);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicPartition, sourceOffset, destinationOffset, lag, state, retryAttempt, errorMessage);
        }

        @Override
        public String toString() {
            return "LeaderStateDescription{" +
                   "topicPartition=" + topicPartition +
                   ", sourceOffset=" + sourceOffset +
                   ", destinationOffset=" + destinationOffset +
                   ", lag=" + lag +
                   ", state='" + state + '\'' +
                   ", retryAttempt=" + retryAttempt +
                   ", errorMessage='" + errorMessage + '\'' +
                   '}';
        }
    }
}
