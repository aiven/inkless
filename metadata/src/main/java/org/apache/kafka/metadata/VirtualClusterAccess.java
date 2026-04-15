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

package org.apache.kafka.metadata;

import java.util.Optional;

import org.apache.kafka.image.TopicLinkImage;
import org.apache.kafka.image.VirtualClusterImage;
import org.apache.kafka.image.VirtualClustersImage;

/**
 * KIP-1134: link-aware access checks for principals assigned to a virtual cluster.
 */
public final class VirtualClusterAccess {

    private VirtualClusterAccess() {
    }

    /**
     * True if the topic string matches a link in this VC (physical topic name or link name).
     */
    public static boolean isTopicInVirtualClusterView(VirtualClusterImage vc, String topicInRequest) {
        for (TopicLinkImage link : vc.topicLinks().values()) {
            if (topicInRequest.equals(link.physicalTopicName()) || topicInRequest.equals(link.linkName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Principals not assigned to any virtual cluster are not restricted by VC metadata.
     */
    public static boolean principalAssignedToVirtualCluster(VirtualClustersImage image, String kafkaPrincipalString) {
        return image.virtualClusterNameForUser(kafkaPrincipalString).isPresent();
    }

    /**
     * Whether a topic may be used when the principal is assigned to a VC: allowed if it appears in that VC's topic links.
     */
    public static boolean isTopicAllowedForPrincipal(
        VirtualClustersImage image,
        String kafkaPrincipalString,
        String topicInRequest
    ) {
        Optional<String> vcName = image.virtualClusterNameForUser(kafkaPrincipalString);
        if (vcName.isEmpty()) {
            return true;
        }
        Optional<VirtualClusterImage> vc = image.virtualCluster(vcName.get());
        return vc.filter(v -> isTopicInVirtualClusterView(v, topicInRequest)).isPresent();
    }

    /**
     * Whether a consumer group id may be used when the principal is assigned to a VC: allowed if listed for that VC.
     */
    public static boolean isGroupAllowedForPrincipal(
        VirtualClustersImage image,
        String kafkaPrincipalString,
        String groupId
    ) {
        Optional<String> vcName = image.virtualClusterNameForUser(kafkaPrincipalString);
        if (vcName.isEmpty()) {
            return true;
        }
        Optional<VirtualClusterImage> vc = image.virtualCluster(vcName.get());
        return vc.map(v -> v.groups().contains(groupId)).orElse(false);
    }
}
