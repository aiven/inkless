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
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.fault.FaultHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;


/**
 * This publisher translates metadata updates sent by MetadataLoader into changes to controller
 * metrics. Like all MetadataPublisher objects, it only receives notifications about events that
 * have been persisted to the metadata log. So on the active controller, it will run slightly
 * behind the latest in-memory state which has not yet been fully persisted to the log. This is
 * reasonable for metrics, which don't need up-to-the-millisecond update latency.
 *
 */
public class ControllerMetadataMetricsPublisher implements MetadataPublisher {
    private static final Logger log = LoggerFactory.getLogger(ControllerMetadataMetricsPublisher.class);

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
        // Use newImage configs to check if topic is diskless, as the metadata cache
        // may not have the config yet when processing deltas for newly created topics
        Function<String, Boolean> isDisklessFromImage = topicName -> {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Properties props = newImage.configs().configProperties(resource);
            return Boolean.parseBoolean(props.getProperty(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));
        };
        ControllerMetricsChanges changes = new ControllerMetricsChanges(isDisklessFromImage);
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
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, prevTopic.name());
                Properties props = prevImage.configs().configProperties(resource);
                boolean isDiskless = Boolean.parseBoolean(props.getProperty(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));
                changes.handleDeletedTopic(prevTopic, isDiskless);
            }
            for (Entry<Uuid, TopicDelta> entry : delta.topicsDelta().changedTopics().entrySet()) {
                changes.handleTopicChange(prevImage.topics().getTopic(entry.getKey()), entry.getValue());
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
        int disklessUnmanagedReplicasTopics = 0;
        int disklessManagedReplicasTopics = 0;
        List<String> unmanagedReplicasTopicNames = new ArrayList<>();
        for (TopicImage topicImage : newImage.topics().topicsById().values()) {
            // Check diskless from newImage configs directly for consistency with delta path
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicImage.name());
            Properties props = newImage.configs().configProperties(resource);
            boolean isDiskless = Boolean.parseBoolean(props.getProperty(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));
            if (isDiskless) {
                disklessTopics++;
                // Check RF from the first partition - all partitions in a topic have the same RF
                if (!topicImage.partitions().isEmpty()) {
                    int rf = topicImage.partitions().values().iterator().next().replicas.length;
                    if (rf > 1) {
                        disklessManagedReplicasTopics++;
                    } else {
                        disklessUnmanagedReplicasTopics++;
                        unmanagedReplicasTopicNames.add(topicImage.name());
                    }
                }
            }
            for (PartitionRegistration partition : topicImage.partitions().values()) {
                if (!isDiskless) {
                    if (!partition.hasLeader()) {
                        offlinePartitions++;
                    }
                    if (!partition.hasPreferredLeader()) {
                        partitionsWithoutPreferredLeader++;
                    }
                }
                totalPartitions++;
            }
        }
        metrics.setGlobalPartitionCount(totalPartitions);
        metrics.setOfflinePartitionCount(offlinePartitions);
        metrics.setPreferredReplicaImbalanceCount(partitionsWithoutPreferredLeader);
        metrics.setDisklessTopicCount(disklessTopics);
        metrics.setDisklessUnmanagedReplicasTopicCount(disklessUnmanagedReplicasTopics);
        metrics.setDisklessManagedReplicasTopicCount(disklessManagedReplicasTopics);

        // Log summary of diskless topics for operational visibility
        if (disklessTopics > 0) {
            log.info("Diskless topics summary: total={}, managed={}, unmanaged={}",
                disklessTopics, disklessManagedReplicasTopics, disklessUnmanagedReplicasTopics);
        }
        if (!unmanagedReplicasTopicNames.isEmpty()) {
            log.info("Diskless topics with unmanaged replicas (RF=1): {}", unmanagedReplicasTopicNames);
        }
    }

    @Override
    public void close() {
        metrics.close();
    }
}
