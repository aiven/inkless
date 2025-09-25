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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;

public class ClusterLinkConfig {

    public static final String CLUSTER_LINK_TOPIC_NUM_PARTITIONS_CONFIG = "cluster.link.topic.num.partitions";
    public static final int CLUSTER_LINK_TOPIC_NUM_PARTITIONS_DEFAULT = 3; // default to 3 for PoC testing
    public static final String CLUSTER_LINK_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions for the cluster link topic (should not change after deployment).";

    public static final String CLUSTER_LINK_TOPIC_REPLICATION_FACTOR_CONFIG = "cluster.link.topic.replication.factor";
    public static final short CLUSTER_LINK_TOPIC_REPLICATION_FACTOR_DEFAULT = 1; // default to 1 for PoC testing
    public static final String CLUSTER_LINK_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor for the cluster link topic. " +
            "Topic creation will fail until the cluster size meets this replication factor requirement.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CLUSTER_LINK_TOPIC_NUM_PARTITIONS_CONFIG, INT, CLUSTER_LINK_TOPIC_NUM_PARTITIONS_DEFAULT, atLeast(1), HIGH, CLUSTER_LINK_TOPIC_NUM_PARTITIONS_DOC)
            .define(CLUSTER_LINK_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, CLUSTER_LINK_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, CLUSTER_LINK_TOPIC_REPLICATION_FACTOR_DOC);


    private final int clusterLinkTopicNumPartitions;
    private final short clusterLinkTopicReplicationFactor;

    public ClusterLinkConfig(AbstractConfig config) {
        clusterLinkTopicNumPartitions = config.getInt(CLUSTER_LINK_TOPIC_NUM_PARTITIONS_CONFIG);
        clusterLinkTopicReplicationFactor = config.getShort(CLUSTER_LINK_TOPIC_REPLICATION_FACTOR_CONFIG);
    }

    public int clusterLinkTopicNumPartitions() {
        return clusterLinkTopicNumPartitions;
    }

    public short clusterLinkTopicReplicationFactor() {
        return clusterLinkTopicReplicationFactor;
    }
}
