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

package org.apache.kafka.controller;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.AlterVirtualClustersRequestData;
import org.apache.kafka.common.message.AlterVirtualClustersResponseData;
import org.apache.kafka.common.message.CreateVirtualClustersRequestData;
import org.apache.kafka.common.message.CreateVirtualClustersResponseData;
import org.apache.kafka.common.message.DeleteVirtualClustersRequestData;
import org.apache.kafka.common.message.DeleteVirtualClustersResponseData;
import org.apache.kafka.common.message.DescribeVirtualClustersRequestData;
import org.apache.kafka.common.message.DescribeVirtualClustersResponseData;
import org.apache.kafka.common.message.ListVirtualClustersResponseData;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RemoveVirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterUserRecord;
import org.apache.kafka.common.metadata.VirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.common.metadata.VirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.VirtualClusterUserRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.TopicLinkImage;
import org.apache.kafka.image.VirtualClusterImage;
import org.apache.kafka.image.VirtualClustersDelta;
import org.apache.kafka.image.VirtualClustersImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.mutable.BoundedList;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineObject;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;

/**
 * Controller logic for KIP-1134 virtual clusters.
 */
public class VirtualClusterControlManager {
    public static final byte RESOURCE_USER = 0;
    public static final byte RESOURCE_TOPIC_LINK = 1;
    public static final byte RESOURCE_GROUP = 2;
    public static final byte RESOURCE_TRANSACTIONAL_ID = 3;
    public static final byte RESOURCE_CLIENT = 4;

    public static final byte OP_ADD = 0;
    public static final byte OP_REMOVE = 1;

    public static class Builder {
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;
        private ReplicationControlManager replicationControl;
        private FeatureControlManager featureControl;

        public Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        public Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        public Builder setReplicationControlManager(ReplicationControlManager replicationControl) {
            this.replicationControl = replicationControl;
            return this;
        }

        public Builder setFeatureControlManager(FeatureControlManager featureControl) {
            this.featureControl = featureControl;
            return this;
        }

        public VirtualClusterControlManager build() {
            if (logContext == null) logContext = new LogContext();
            return new VirtualClusterControlManager(logContext, snapshotRegistry, replicationControl, featureControl);
        }
    }

    private final Logger log;
    private final SnapshotRegistry snapshotRegistry;
    private final ReplicationControlManager replicationControl;
    private final FeatureControlManager featureControl;
    private final TimelineObject<VirtualClustersImage> virtualClusters;

    private VirtualClusterControlManager(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        ReplicationControlManager replicationControl,
        FeatureControlManager featureControl
    ) {
        this.log = logContext.logger(VirtualClusterControlManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.replicationControl = replicationControl;
        this.featureControl = featureControl;
        this.virtualClusters = new TimelineObject<>(snapshotRegistry, VirtualClustersImage.EMPTY);
    }

    private void checkSupported() {
        MetadataVersion v = featureControl.metadataVersionOrThrow();
        if (!v.isVirtualClusterSupported()) {
            throw new InvalidRequestException("metadata.version must be at least " + MetadataVersion.IBP_4_2_IV2 + " for virtual clusters.");
        }
    }

    private void applyDelta(java.util.function.Consumer<VirtualClustersDelta> fn) {
        VirtualClustersDelta delta = new VirtualClustersDelta(virtualClusters.get());
        fn.accept(delta);
        virtualClusters.set(delta.apply());
    }

    public void replay(ApiMessage message) {
        applyDelta(delta -> {
            if (message instanceof VirtualClusterRecord r) delta.replay(r);
            else if (message instanceof RemoveVirtualClusterRecord r) delta.replay(r);
            else if (message instanceof VirtualClusterTopicLinkRecord r) delta.replay(r);
            else if (message instanceof RemoveVirtualClusterTopicLinkRecord r) delta.replay(r);
            else if (message instanceof VirtualClusterUserRecord r) delta.replay(r);
            else if (message instanceof RemoveVirtualClusterUserRecord r) delta.replay(r);
            else if (message instanceof VirtualClusterGroupRecord r) delta.replay(r);
            else if (message instanceof RemoveVirtualClusterGroupRecord r) delta.replay(r);
            else throw new RuntimeException("Unsupported virtual cluster record " + message.getClass());
        });
    }

    public VirtualClustersImage image() {
        return virtualClusters.get();
    }

    public ControllerResult<CreateVirtualClustersResponseData> createVirtualClusters(
        ControllerRequestContext context,
        CreateVirtualClustersRequestData request
    ) {
        checkSupported();
        CreateVirtualClustersResponseData response = new CreateVirtualClustersResponseData();
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        Set<String> seenInRequest = new HashSet<>();
        for (CreateVirtualClustersRequestData.CreatableVirtualCluster vc : request.virtualClusters()) {
            CreateVirtualClustersResponseData.CreatableVirtualClustersResult result =
                new CreateVirtualClustersResponseData.CreatableVirtualClustersResult().setName(vc.name());
            try {
                validateVcName(vc.name());
                if (!seenInRequest.add(vc.name())) {
                    throw new InvalidRequestException("Duplicate virtual cluster name " + vc.name() + " in request.");
                }
                if (virtualClusters.get().virtualCluster(vc.name()).isPresent()) {
                    throw new InvalidRequestException("Virtual cluster " + vc.name() + " already exists.");
                }
                if (!request.validateOnly()) {
                    records.add(new ApiMessageAndVersion(
                        new VirtualClusterRecord().setName(vc.name()),
                        MetadataRecordType.VIRTUAL_CLUSTER_RECORD.highestSupportedVersion()));
                }
                result.setErrorCode(Errors.NONE.code());
            } catch (Exception e) {
                result.setErrorCode(Errors.forException(e).code());
                result.setErrorMessage(e.getMessage());
            }
            response.virtualClusters().add(result);
        }
        if (request.validateOnly()) {
            return ControllerResult.of(List.of(), response);
        }
        return ControllerResult.atomicOf(records, response);
    }

    public ControllerResult<AlterVirtualClustersResponseData> alterVirtualClusters(
        ControllerRequestContext context,
        AlterVirtualClustersRequestData request
    ) {
        checkSupported();
        AlterVirtualClustersResponseData response = new AlterVirtualClustersResponseData();
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        for (AlterVirtualClustersRequestData.AlterableVirtualCluster vcData : request.virtualClusters()) {
            AlterVirtualClustersResponseData.AlterableVirtualClusterResult vcResult =
                new AlterVirtualClustersResponseData.AlterableVirtualClusterResult().setName(vcData.name());
            for (AlterVirtualClustersRequestData.VirtualClusterResourceChange res : vcData.resources()) {
                AlterVirtualClustersResponseData.VirtualClusterResourceResult rr =
                    new AlterVirtualClustersResponseData.VirtualClusterResourceResult()
                        .setResourceType(res.resourceType())
                        .setResourceOperation(res.resourceOperation())
                        .setResourceName(res.resourceName());
                try {
                    appendAlterRecords(vcData.name(), res, records, request.validateOnly());
                    rr.setErrorCode(Errors.NONE.code());
                } catch (Exception e) {
                    rr.setErrorCode(Errors.forException(e).code());
                    rr.setErrorMessage(e.getMessage());
                }
                vcResult.resources().add(rr);
            }
            response.virtualClusters().add(vcResult);
        }
        if (request.validateOnly()) {
            return ControllerResult.of(List.of(), response);
        }
        return ControllerResult.atomicOf(records, response);
    }

    private void appendAlterRecords(
        String vcName,
        AlterVirtualClustersRequestData.VirtualClusterResourceChange res,
        List<ApiMessageAndVersion> records,
        boolean validateOnly
    ) {
        VirtualClustersImage img = virtualClusters.get();
        if (img.virtualCluster(vcName).isEmpty()) {
            throw new InvalidRequestException("Virtual cluster " + vcName + " does not exist.");
        }
        byte type = res.resourceType();
        byte op = res.resourceOperation();
        if (op != OP_ADD && op != OP_REMOVE) {
            throw new InvalidRequestException("Invalid resource operation " + op);
        }
        if (type == RESOURCE_USER) {
            String user = res.resourceName();
            if (op == OP_ADD) {
                Optional<String> other = img.virtualClusterNameForUser(user);
                if (other.isPresent() && !other.get().equals(vcName)) {
                    throw new InvalidRequestException("User " + user + " is already assigned to virtual cluster " + other.get());
                }
                if (!validateOnly) {
                    records.add(new ApiMessageAndVersion(
                        new VirtualClusterUserRecord().setVirtualClusterName(vcName).setUserName(user),
                        MetadataRecordType.VIRTUAL_CLUSTER_USER_RECORD.highestSupportedVersion()));
                }
            } else {
                if (!validateOnly) {
                    records.add(new ApiMessageAndVersion(
                        new RemoveVirtualClusterUserRecord().setUserName(user),
                        MetadataRecordType.REMOVE_VIRTUAL_CLUSTER_USER_RECORD.highestSupportedVersion()));
                }
            }
        } else if (type == RESOURCE_GROUP) {
            String group = res.resourceName();
            if (op == OP_ADD) {
                Optional<String> other = img.virtualClusterNameForGroup(group);
                if (other.isPresent() && !other.get().equals(vcName)) {
                    throw new InvalidRequestException("Group " + group + " is already assigned to virtual cluster " + other.get());
                }
                if (!validateOnly) {
                    records.add(new ApiMessageAndVersion(
                        new VirtualClusterGroupRecord().setVirtualClusterName(vcName).setGroupId(group),
                        MetadataRecordType.VIRTUAL_CLUSTER_GROUP_RECORD.highestSupportedVersion()));
                }
            } else {
                if (!validateOnly) {
                    records.add(new ApiMessageAndVersion(
                        new RemoveVirtualClusterGroupRecord().setGroupId(group),
                        MetadataRecordType.REMOVE_VIRTUAL_CLUSTER_GROUP_RECORD.highestSupportedVersion()));
                }
            }
        } else if (type == RESOURCE_TOPIC_LINK) {
            String linkName = res.resourceName();
            String physical = res.physicalTopicName();
            if (op == OP_ADD) {
                if (physical == null || physical.isEmpty()) {
                    throw new InvalidRequestException("physicalTopicName is required for topic link add.");
                }
                if (replicationControl.getTopicId(physical) == null) {
                    throw new InvalidRequestException("Unknown topic " + physical);
                }
                if (!validateOnly) {
                    records.add(new ApiMessageAndVersion(
                        new VirtualClusterTopicLinkRecord()
                            .setVirtualClusterName(vcName)
                            .setLinkName(linkName)
                            .setPhysicalTopicName(physical),
                        MetadataRecordType.VIRTUAL_CLUSTER_TOPIC_LINK_RECORD.highestSupportedVersion()));
                }
            } else {
                if (!validateOnly) {
                    records.add(new ApiMessageAndVersion(
                        new RemoveVirtualClusterTopicLinkRecord()
                            .setVirtualClusterName(vcName)
                            .setLinkName(linkName),
                        MetadataRecordType.REMOVE_VIRTUAL_CLUSTER_TOPIC_LINK_RECORD.highestSupportedVersion()));
                }
            }
        } else if (type == RESOURCE_CLIENT || type == RESOURCE_TRANSACTIONAL_ID) {
            throw new InvalidRequestException("Resource type " + type + " is not supported yet.");
        } else {
            throw new InvalidRequestException("Invalid resource type " + type);
        }
    }

    public ControllerResult<DeleteVirtualClustersResponseData> deleteVirtualClusters(
        ControllerRequestContext context,
        DeleteVirtualClustersRequestData request
    ) {
        checkSupported();
        DeleteVirtualClustersResponseData response = new DeleteVirtualClustersResponseData();
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        for (DeleteVirtualClustersRequestData.DeletableVirtualCluster d : request.virtualClusters()) {
            DeleteVirtualClustersResponseData.DeletableVirtualClusterResult r =
                new DeleteVirtualClustersResponseData.DeletableVirtualClusterResult().setName(d.name());
            try {
                VirtualClusterImage vc = virtualClusters.get().virtualCluster(d.name()).orElseThrow(
                    () -> new InvalidRequestException("Virtual cluster " + d.name() + " does not exist."));
                if (!vc.isEmpty()) {
                    throw new InvalidRequestException("Virtual cluster " + d.name() + " is not empty.");
                }
                if (!request.validateOnly()) {
                    records.add(new ApiMessageAndVersion(
                        new RemoveVirtualClusterRecord().setName(d.name()),
                        MetadataRecordType.REMOVE_VIRTUAL_CLUSTER_RECORD.highestSupportedVersion()));
                }
                r.setErrorCode(Errors.NONE.code());
            } catch (Exception e) {
                r.setErrorCode(Errors.forException(e).code());
                r.setErrorMessage(e.getMessage());
            }
            response.responses().add(r);
        }
        if (request.validateOnly()) {
            return ControllerResult.of(List.of(), response);
        }
        return ControllerResult.atomicOf(records, response);
    }

    public ControllerResult<ListVirtualClustersResponseData> listVirtualClusters(ControllerRequestContext context) {
        checkSupported();
        ListVirtualClustersResponseData data = new ListVirtualClustersResponseData()
            .setErrorCode(Errors.NONE.code());
        List<String> names = new ArrayList<>(virtualClusters.get().clusters().keySet());
        Collections.sort(names);
        for (String n : names) {
            data.virtualClusters().add(new ListVirtualClustersResponseData.ListedVirtualCluster().setName(n));
        }
        return ControllerResult.of(List.of(), data);
    }

    public ControllerResult<DescribeVirtualClustersResponseData> describeVirtualClusters(
        ControllerRequestContext context,
        DescribeVirtualClustersRequestData request
    ) {
        checkSupported();
        DescribeVirtualClustersResponseData response = new DescribeVirtualClustersResponseData();
        for (DescribeVirtualClustersRequestData.DescribableVirtualCluster d : request.virtualClusters()) {
            DescribeVirtualClustersResponseData.DescribedVirtualCluster dr =
                new DescribeVirtualClustersResponseData.DescribedVirtualCluster().setName(d.name());
            Optional<VirtualClusterImage> vcOpt = virtualClusters.get().virtualCluster(d.name());
            if (vcOpt.isEmpty()) {
                dr.setErrorCode(Errors.RESOURCE_NOT_FOUND.code());
            } else {
                VirtualClusterImage vc = vcOpt.get();
                dr.setErrorCode(Errors.NONE.code());
                for (TopicLinkImage link : vc.topicLinks().values()) {
                    dr.topicLinks().add(new DescribeVirtualClustersResponseData.VirtualClusterTopicLink()
                        .setLinkName(link.linkName())
                        .setPhysicalName(link.physicalTopicName()));
                }
                vc.users().stream().sorted().forEach(u -> dr.users().add(u));
                vc.groups().stream().sorted().forEach(g -> dr.consumerGroups().add(g));
            }
            response.virtualClusters().add(dr);
        }
        return ControllerResult.of(List.of(), response);
    }

    private static void validateVcName(String name) {
        if (name == null || name.isEmpty()) {
            throw new InvalidRequestException("Invalid virtual cluster name.");
        }
    }
}
