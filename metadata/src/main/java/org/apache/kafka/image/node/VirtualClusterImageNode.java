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

import org.apache.kafka.image.TopicLinkImage;
import org.apache.kafka.image.VirtualClusterImage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class VirtualClusterImageNode implements MetadataNode {
    public static final String TOPIC_LINKS = "topicLinks";
    public static final String USERS = "users";
    public static final String GROUPS = "groups";

    private final VirtualClusterImage image;

    public VirtualClusterImageNode(VirtualClusterImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        return List.of(TOPIC_LINKS, USERS, GROUPS);
    }

    @Override
    public MetadataNode child(String name) {
        return switch (name) {
            case TOPIC_LINKS -> new VirtualClusterTopicLinksNode(image);
            case USERS -> new VirtualClusterStringSetNode(USERS, image.users());
            case GROUPS -> new VirtualClusterStringSetNode(GROUPS, image.groups());
            default -> null;
        };
    }
}

final class VirtualClusterTopicLinksNode implements MetadataNode {
    private final VirtualClusterImage image;

    VirtualClusterTopicLinksNode(VirtualClusterImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        ArrayList<String> names = new ArrayList<>(image.topicLinks().keySet());
        Collections.sort(names);
        return names;
    }

    @Override
    public MetadataNode child(String name) {
        TopicLinkImage link = image.topicLinks().get(name);
        if (link == null) {
            return null;
        }
        return new MetadataLeafNode(link.toString());
    }
}

final class VirtualClusterStringSetNode implements MetadataNode {
    private final String label;
    private final Collection<String> values;

    VirtualClusterStringSetNode(String label, Collection<String> values) {
        this.label = label;
        this.values = values;
    }

    @Override
    public Collection<String> childNames() {
        ArrayList<String> names = new ArrayList<>(values);
        Collections.sort(names);
        return names;
    }

    @Override
    public MetadataNode child(String name) {
        if (!values.contains(name)) {
            return null;
        }
        return new MetadataLeafNode(label + ":" + name);
    }
}
