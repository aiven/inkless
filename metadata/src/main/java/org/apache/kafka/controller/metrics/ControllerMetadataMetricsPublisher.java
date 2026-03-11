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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.fault.FaultHandler;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;


/**
 * This publisher translates metadata updates sent by MetadataLoader into changes to controller
 * metrics. Like all MetadataPublisher objects, it only receives notifications about events that
 * have been persisted to the metadata log. So on the active controller, it will run slightly
 * behind the latest in-memory state which has not yet been fully persisted to the log. This is
 * reasonable for metrics, which don't need up-to-the-millisecond update latency.
 *
 */
public class ControllerMetadataMetricsPublisher implements MetadataPublisher {
    private final ControllerMetadataMetrics metrics;
    private final FaultHandler faultHandler;
    private MetadataImage prevImage = MetadataImage.EMPTY;

    public ControllerMetadataMetricsPublisher(
        ControllerMetadataMetrics metrics,
        FaultHandler faultHandler
    ) {
        this.metrics = metrics;
        this.faultHandler = faultHandler;
    }

    @Override
    public String name() {
        return "ControllerMetadataMetricsPublisher";
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        switch (manifest.type()) {
            case LOG_DELTA:
                try {
                    publishDelta(delta, newImage);
                } catch (Throwable e) {
                    faultHandler.handleFault("Failed to publish controller metrics from log delta " +
                            " ending at offset " + manifest.provenance().lastContainedOffset(), e);
                } finally {
                    prevImage = newImage;
                }
                break;
            case SNAPSHOT:
                try {
                    publishSnapshot(newImage);
                } catch (Throwable e) {
                    faultHandler.handleFault("Failed to publish controller metrics from " +
                            manifest.provenance().snapshotName(), e);
                } finally {
                    prevImage = newImage;
                }
                break;
        }
    }

    private void publishDelta(MetadataDelta delta, MetadataImage newImage) {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        if (delta.clusterDelta() != null) {
            for (Entry<Integer, Optional<BrokerRegistration>> entry :
                    delta.clusterDelta().changedBrokers().entrySet()) {
                changes.handleBrokerChange(
                    prevImage.cluster().brokers().get(entry.getKey()),
                    entry.getValue().orElse(null),
                    metrics
                );
            }
        }
        if (delta.topicsDelta() != null) {
            for (Uuid topicId : delta.topicsDelta().deletedTopicIds()) {
                TopicImage prevTopic = prevImage.topics().topicsById().get(topicId);
                if (prevTopic == null) {
                    throw new RuntimeException("Unable to find deleted topic id " + topicId +
                            " in previous topics image.");
                }
                // For deleted topics, check isDiskless from prevImage since config is already removed from newImage
                changes.handleDeletedTopic(prevTopic, isDisklessTopic(prevImage.configs(), prevTopic.name()));
            }
            for (Entry<Uuid, TopicDelta> entry : delta.topicsDelta().changedTopics().entrySet()) {
                changes.handleTopicChange(
                    prevImage.topics().getTopic(entry.getKey()),
                    entry.getValue(),
                    isDisklessTopic(newImage.configs(), entry.getValue().name()));
            }
        }
        // Handle diskless.enable config changes on existing topics (no TopicDelta required).
        // Recategorizes partitions between classic and diskless metric buckets.
        if (delta.configsDelta() != null) {
            for (ConfigResource resource : delta.configsDelta().changes().keySet()) {
                if (resource.type() != ConfigResource.Type.TOPIC) continue;
                boolean wasDiskless = isDisklessTopic(prevImage.configs(), resource.name());
                boolean isDiskless = isDisklessTopic(newImage.configs(), resource.name());
                if (wasDiskless != isDiskless) {
                    TopicImage topic = newImage.topics().getTopic(resource.name());
                    if (topic != null) {
                        changes.handleDisklessConfigChange(topic, isDiskless);
                    }
                }
            }
        }
        changes.apply(metrics);
    }

    private void publishSnapshot(MetadataImage newImage) {
        metrics.setGlobalTopicCount(newImage.topics().topicsById().size());
        int fencedBrokers = 0;
        int activeBrokers = 0;
        int controlledShutdownBrokers = 0;
        for (BrokerRegistration broker : newImage.cluster().brokers().values()) {
            if (broker.fenced()) {
                fencedBrokers++;
            } else {
                activeBrokers++;
            }
            if (broker.inControlledShutdown()) {
                controlledShutdownBrokers++;
            }
        }
        metrics.setFencedBrokerCount(fencedBrokers);
        metrics.setActiveBrokerCount(activeBrokers);
        metrics.setControlledShutdownBrokerCount(controlledShutdownBrokers);

        int totalPartitions = 0;
        int offlinePartitions = 0;
        int partitionsWithoutPreferredLeader = 0;
        int disklessTopics = 0;
        int disklessPartitions = 0;
        int disklessOfflinePartitions = 0;
        for (TopicImage topicImage : newImage.topics().topicsById().values()) {
            // Check diskless from newImage configs directly for consistency with delta path
            final boolean isDiskless = isDisklessTopic(newImage.configs(), topicImage.name());
            if (isDiskless) {
                disklessTopics++;
            }
            for (PartitionRegistration partition : topicImage.partitions().values()) {
                totalPartitions++;
                if (isDiskless) {
                    disklessPartitions++;
                    if (!partition.hasLeader()) {
                        disklessOfflinePartitions++;
                    }
                } else {
                    if (!partition.hasLeader()) {
                        offlinePartitions++;
                    }
                    if (!partition.hasPreferredLeader()) {
                        partitionsWithoutPreferredLeader++;
                    }
                }
            }
        }
        metrics.setGlobalPartitionCount(totalPartitions);
        metrics.setOfflinePartitionCount(offlinePartitions);
        metrics.setPreferredReplicaImbalanceCount(partitionsWithoutPreferredLeader);
        metrics.setDisklessTopicCount(disklessTopics);
        metrics.setDisklessPartitionCount(disklessPartitions);
        metrics.setDisklessOfflinePartitionCount(disklessOfflinePartitions);
    }

    private static boolean isDisklessTopic(ConfigurationsImage configsImage, String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<String, String> configMap = configsImage.configMapForResource(resource);
        return Boolean.parseBoolean(configMap.getOrDefault(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));
    }

    @Override
    public void close() {
        metrics.close();
    }
}
