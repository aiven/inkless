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
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.List;
import java.util.Set;

public interface MetadataView {
    List<Node> getAliveBrokerNodes(ListenerName listenerName);

    Integer getBrokerCount();

    Uuid getTopicId(String topicName);

    /**
     * Checks whether the topic has the {@code diskless.enable=true} topic override.
     *
     * <p>Only the topic override counts, so an unset key is false.
     * When the broker default makes a new topic diskless, the controller writes the override for that topic.
     * Callers can make this call for each partition, since it doesn't copy the topic config.
     *
     * @return True if the topic override is {@code true}, ignoring case; false otherwise.
     * @throws IllegalArgumentException if the override is a value such as {@code " true"},
     *     which isn't {@code true} or {@code false}, ignoring case, so the topic type is unknown.
     */
    boolean isDisklessTopic(String topicName);

    /**
     * Checks whether the topic has the {@code remote.storage.enable=true} topic override.
     * The same rules apply as for {@link #isDisklessTopic(String)}.
     *
     * @return True if the topic override is {@code true}, ignoring case; false otherwise.
     */
    boolean isRemoteStorageEnabled(String topicName);

    /**
     * Checks whether the topic is diskless with remote storage enabled.
     * Both overrides come from the same metadata snapshot, so a concurrent config change can't mix two snapshots.
     *
     * @return True if both {@link #isDisklessTopic(String)} and {@link #isRemoteStorageEnabled(String)}
     *     are true on the same snapshot; false otherwise.
     */
    boolean isConsolidatingDisklessTopic(String topicName);

    /**
     * Checks whether the partition is in a classic-to-diskless switch and its classic prefix lives only
     * on the replicas' local disks.
     *
     * @return true if the classic-to-diskless switch is pending, or if the controller committed the seal
     *     and {@code classicPrefixInRemoteStorage} is unset; false otherwise.
     */
    boolean isClassicPrefixOnlyOnLocalDisks(String topicName, int partition);

    LogConfig getTopicConfig(String topicName);

    Set<TopicIdPartition> getDisklessTopicPartitions();

    Set<TopicIdPartition> getConsolidatingDisklessTopicPartitions();
}
