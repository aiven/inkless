/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public interface MetadataView {
    Map<String, Object> getDefaultConfig();

    List<Node> getAliveBrokerNodes(ListenerName listenerName);

    Integer getBrokerCount();

    Uuid getTopicId(String topicName);

    boolean isDisklessTopic(String topicName);

    Properties getTopicConfig(String topicName);

    Set<TopicIdPartition> getDisklessTopicPartitions();
}
