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

import org.apache.kafka.common.metadata.VirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.common.metadata.VirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.VirtualClusterUserRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.VirtualClustersImage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VirtualClusterAccessTest {

    private static VirtualClustersImage imageWithUserAndLinks(
        String vcName,
        String user,
        String linkName,
        String physicalTopic
    ) {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new VirtualClusterRecord().setName(vcName));
        delta.replay(new VirtualClusterTopicLinkRecord()
            .setVirtualClusterName(vcName)
            .setLinkName(linkName)
            .setPhysicalTopicName(physicalTopic));
        delta.replay(new VirtualClusterUserRecord()
            .setVirtualClusterName(vcName)
            .setUserName(user));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        return image.virtualClusters();
    }

    @Test
    public void principalNotInVcAlwaysAllowedForTopic() {
        VirtualClustersImage image = imageWithUserAndLinks("vc1", "User:Alice", "l1", "phys");
        assertTrue(VirtualClusterAccess.isTopicAllowedForPrincipal(image, "User:Bob", "phys"));
    }

    @Test
    public void topicDeniedWhenOnlyLinkedInOtherVc() {
        VirtualClustersImage image = imageWithUserAndLinks("vc1", "User:Alice", "l1", "phys-a");
        assertFalse(VirtualClusterAccess.isTopicAllowedForPrincipal(image, "User:Alice", "other-topic"));
    }

    @Test
    public void topicAllowedByPhysicalNameOrLinkName() {
        VirtualClustersImage image = imageWithUserAndLinks("vc1", "User:Alice", "mylink", "phys-a");
        assertTrue(VirtualClusterAccess.isTopicAllowedForPrincipal(image, "User:Alice", "phys-a"));
        assertTrue(VirtualClusterAccess.isTopicAllowedForPrincipal(image, "User:Alice", "mylink"));
    }

    @Test
    public void samePhysicalTopicLinkedInTwoVcs() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new VirtualClusterRecord().setName("vcA"));
        delta.replay(new VirtualClusterRecord().setName("vcB"));
        delta.replay(new VirtualClusterTopicLinkRecord()
            .setVirtualClusterName("vcA")
            .setLinkName("l")
            .setPhysicalTopicName("shared"));
        delta.replay(new VirtualClusterTopicLinkRecord()
            .setVirtualClusterName("vcB")
            .setLinkName("l")
            .setPhysicalTopicName("shared"));
        delta.replay(new VirtualClusterUserRecord()
            .setVirtualClusterName("vcA")
            .setUserName("User:Alice"));
        delta.replay(new VirtualClusterUserRecord()
            .setVirtualClusterName("vcB")
            .setUserName("User:Bob"));
        VirtualClustersImage merged = delta.apply(MetadataProvenance.EMPTY).virtualClusters();

        assertTrue(VirtualClusterAccess.isTopicAllowedForPrincipal(merged, "User:Alice", "shared"));
        assertTrue(VirtualClusterAccess.isTopicAllowedForPrincipal(merged, "User:Bob", "shared"));
    }

    @Test
    public void groupAllowedOnlyWhenRegisteredInVc() {
        VirtualClustersImage image = imageWithUserAndLinks("vc1", "User:Alice", "l", "t");
        assertFalse(VirtualClusterAccess.isGroupAllowedForPrincipal(image, "User:Alice", "g1"));
    }

    @Test
    public void groupAllowedWhenRegisteredInVc() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new VirtualClusterRecord().setName("vc1"));
        delta.replay(new VirtualClusterUserRecord()
            .setVirtualClusterName("vc1")
            .setUserName("User:Alice"));
        delta.replay(new VirtualClusterGroupRecord()
            .setVirtualClusterName("vc1")
            .setGroupId("mygroup"));
        VirtualClustersImage image = delta.apply(MetadataProvenance.EMPTY).virtualClusters();
        assertTrue(VirtualClusterAccess.isGroupAllowedForPrincipal(image, "User:Alice", "mygroup"));
    }
}
