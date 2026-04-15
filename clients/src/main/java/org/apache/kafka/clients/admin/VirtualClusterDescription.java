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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Description of a virtual cluster returned from {@link Admin#describeVirtualClusters} (KIP-1134).
 */
public class VirtualClusterDescription {
    private final String name;
    private final Map<String, String> topicLinks;
    private final Set<String> users;
    private final Set<String> consumerGroups;

    public VirtualClusterDescription(
        String name,
        Map<String, String> topicLinks,
        Set<String> users,
        Set<String> consumerGroups
    ) {
        this.name = Objects.requireNonNull(name);
        this.topicLinks = Collections.unmodifiableMap(topicLinks);
        this.users = Collections.unmodifiableSet(users);
        this.consumerGroups = Collections.unmodifiableSet(consumerGroups);
    }

    public String name() {
        return name;
    }

    /** Link name to physical topic name. */
    public Map<String, String> topicLinks() {
        return topicLinks;
    }

    public Set<String> users() {
        return users;
    }

    public Set<String> consumerGroups() {
        return consumerGroups;
    }
}
