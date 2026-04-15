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

package org.apache.kafka.image.node;

import org.apache.kafka.image.MetadataImage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


/**
 * @param image The metadata image.
 */
public record MetadataImageNode(MetadataImage image) implements MetadataNode {
    /**
     * The name of this node.
     */
    public static final String NAME = "image";

    private static final Map<String, Function<MetadataImage, MetadataNode>> CHILDREN = createChildren();

    private static Map<String, Function<MetadataImage, MetadataNode>> createChildren() {
        Map<String, Function<MetadataImage, MetadataNode>> m = new HashMap<>();
        m.put(ProvenanceNode.NAME, image -> new ProvenanceNode(image.provenance()));
        m.put(FeaturesImageNode.NAME, image -> new FeaturesImageNode(image.features()));
        m.put(ClusterImageNode.NAME, image -> new ClusterImageNode(image.cluster()));
        m.put(TopicsImageNode.NAME, image -> new TopicsImageNode(image.topics()));
        m.put(ConfigurationsImageNode.NAME, image -> new ConfigurationsImageNode(image.configs()));
        m.put(ClientQuotasImageNode.NAME, image -> new ClientQuotasImageNode(image.clientQuotas()));
        m.put(ProducerIdsImageNode.NAME, image -> new ProducerIdsImageNode(image.producerIds()));
        m.put(AclsImageNode.NAME, image -> new AclsImageNode(image.acls()));
        m.put(ScramImageNode.NAME, image -> new ScramImageNode(image.scram()));
        m.put(DelegationTokenImageNode.NAME, image -> new DelegationTokenImageNode(image.delegationTokens()));
        m.put(VirtualClustersImageNode.NAME, image -> new VirtualClustersImageNode(image.virtualClusters()));
        return Map.copyOf(m);
    }

    @Override
    public Collection<String> childNames() {
        return CHILDREN.keySet();
    }

    @Override
    public MetadataNode child(String name) {
        return CHILDREN.getOrDefault(name, __ -> null).apply(image);
    }
}
