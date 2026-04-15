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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AlterVirtualClustersRequestData;
import org.apache.kafka.common.message.CreateVirtualClustersRequestData;
import org.apache.kafka.common.message.DeleteVirtualClustersRequestData;
import org.apache.kafka.common.message.DescribeVirtualClustersRequestData;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.RemoveVirtualClusterUserRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.VirtualClusterGroupRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.common.metadata.VirtualClusterTopicLinkRecord;
import org.apache.kafka.common.metadata.VirtualClusterUserRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.util.MockRandom;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.FakeKafkaConfigSchema;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VirtualClusterControlManagerTest {

    private static final long BROKER_SESSION_TIMEOUT_MS = 1000L;

    static class Fixture implements AutoCloseable {
        final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        final LogContext logContext = new LogContext();
        final MockTime time = new MockTime();
        final MockRandom random = new MockRandom();
        final FeatureControlManager featureControl;
        final ClusterControlManager clusterControl;
        final ConfigurationControlManager configurationControl;
        final ReplicationControlManager replicationControl;
        final VirtualClusterControlManager virtualClusters;

        Fixture() {
            featureControl = new FeatureControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setQuorumFeatures(new QuorumFeatures(0,
                    QuorumFeatures.defaultSupportedFeatureMap(true),
                    List.of(0))).
                build();
            featureControl.replay(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(MetadataVersion.IBP_4_2_IV2.featureLevel()));
            clusterControl = new ClusterControlManager.Builder().
                setLogContext(logContext).
                setTime(time).
                setSnapshotRegistry(snapshotRegistry).
                setSessionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(BROKER_SESSION_TIMEOUT_MS)).
                setReplicaPlacer(new StripedReplicaPlacer(random)).
                setFeatureControlManager(featureControl).
                setBrokerShutdownHandler((a, b, c) -> { }).
                build();
            configurationControl = new ConfigurationControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setFeatureControl(featureControl).
                setStaticConfig(Map.of()).
                setKafkaConfigSchema(FakeKafkaConfigSchema.INSTANCE).
                build();
            replicationControl = new ReplicationControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setLogContext(logContext).
                setMaxElectionsPerImbalance(Integer.MAX_VALUE).
                setConfigurationControl(configurationControl).
                setClusterControl(clusterControl).
                setFeatureControl(featureControl).
                build();
            virtualClusters = new VirtualClusterControlManager.Builder().
                setLogContext(logContext).
                setSnapshotRegistry(snapshotRegistry).
                setReplicationControlManager(replicationControl).
                setFeatureControlManager(featureControl).
                build();
            clusterControl.activate();
        }

        void replay(List<ApiMessageAndVersion> records) {
            RecordTestUtils.replayAll(clusterControl, records);
            RecordTestUtils.replayAll(configurationControl, records);
            RecordTestUtils.replayAll(replicationControl, records);
            for (ApiMessageAndVersion record : records) {
                org.apache.kafka.common.protocol.ApiMessage msg = record.message();
                if (isVirtualClusterMetadataRecord(msg)) {
                    virtualClusters.replay(msg);
                }
            }
        }

        private static boolean isVirtualClusterMetadataRecord(org.apache.kafka.common.protocol.ApiMessage msg) {
            return isVirtualClusterAddRecord(msg) || isVirtualClusterRemoveRecord(msg);
        }

        private static boolean isVirtualClusterAddRecord(org.apache.kafka.common.protocol.ApiMessage msg) {
            return msg instanceof VirtualClusterRecord
                || msg instanceof VirtualClusterTopicLinkRecord
                || msg instanceof VirtualClusterUserRecord
                || msg instanceof VirtualClusterGroupRecord;
        }

        private static boolean isVirtualClusterRemoveRecord(org.apache.kafka.common.protocol.ApiMessage msg) {
            return msg instanceof RemoveVirtualClusterRecord
                || msg instanceof RemoveVirtualClusterTopicLinkRecord
                || msg instanceof RemoveVirtualClusterUserRecord
                || msg instanceof RemoveVirtualClusterGroupRecord;
        }

        @Override
        public void close() {
            clusterControl.deactivate();
        }
    }

    private static ControllerRequestContext ctx(ApiKeys key) {
        return new ControllerRequestContext(
            new org.apache.kafka.common.requests.RequestHeader(key, key.latestVersion(), "", 1).data(),
            org.apache.kafka.common.security.auth.KafkaPrincipal.ANONYMOUS,
            java.util.OptionalLong.empty());
    }

    @Test
    public void testCreateAlterDescribeDelete() throws Exception {
        try (Fixture f = new Fixture()) {
            Uuid t1 = Uuid.randomUuid();
            f.replay(List.of(new ApiMessageAndVersion(
                new TopicRecord().setName("t1").setTopicId(t1),
                MetadataRecordType.TOPIC_RECORD.highestSupportedVersion())));

            ControllerResult<org.apache.kafka.common.message.CreateVirtualClustersResponseData> vc =
                f.virtualClusters.createVirtualClusters(ctx(ApiKeys.CREATE_VIRTUAL_CLUSTERS),
                    new CreateVirtualClustersRequestData().
                        setVirtualClusters(new CreateVirtualClustersRequestData.CreatableVirtualClusterCollection(
                            List.of(new CreateVirtualClustersRequestData.CreatableVirtualCluster().setName("vc1")).iterator())));
            assertTrue(vc.records().size() > 0);
            f.replay(vc.records());
            assertEquals(Errors.NONE, Errors.forCode(vc.response().virtualClusters().find("vc1").errorCode()));

            AlterVirtualClustersRequestData.AlterableVirtualCluster avcAdd =
                new AlterVirtualClustersRequestData.AlterableVirtualCluster().setName("vc1");
            avcAdd.resources().add(new AlterVirtualClustersRequestData.VirtualClusterResourceChange().
                setResourceType(VirtualClusterControlManager.RESOURCE_TOPIC_LINK).
                setResourceOperation(VirtualClusterControlManager.OP_ADD).
                setResourceName("lnk1").
                setPhysicalTopicName("t1"));
            avcAdd.resources().add(new AlterVirtualClustersRequestData.VirtualClusterResourceChange().
                setResourceType(VirtualClusterControlManager.RESOURCE_USER).
                setResourceOperation(VirtualClusterControlManager.OP_ADD).
                setResourceName("User:alice"));
            avcAdd.resources().add(new AlterVirtualClustersRequestData.VirtualClusterResourceChange().
                setResourceType(VirtualClusterControlManager.RESOURCE_GROUP).
                setResourceOperation(VirtualClusterControlManager.OP_ADD).
                setResourceName("g1"));
            AlterVirtualClustersRequestData.AlterableVirtualClusterCollection altColl =
                new AlterVirtualClustersRequestData.AlterableVirtualClusterCollection();
            altColl.add(avcAdd);
            ControllerResult<org.apache.kafka.common.message.AlterVirtualClustersResponseData> alt =
                f.virtualClusters.alterVirtualClusters(ctx(ApiKeys.ALTER_VIRTUAL_CLUSTERS),
                    new AlterVirtualClustersRequestData().setVirtualClusters(altColl));
            f.replay(alt.records());

            ControllerResult<org.apache.kafka.common.message.DescribeVirtualClustersResponseData> desc =
                f.virtualClusters.describeVirtualClusters(ctx(ApiKeys.DESCRIBE_VIRTUAL_CLUSTERS),
                    new DescribeVirtualClustersRequestData().
                        setVirtualClusters(new DescribeVirtualClustersRequestData.DescribableVirtualClusterCollection(
                            List.of(new DescribeVirtualClustersRequestData.DescribableVirtualCluster().setName("vc1")).iterator())));
            var d = desc.response().virtualClusters().find("vc1");
            assertEquals(Errors.NONE, Errors.forCode(d.errorCode()));
            assertEquals("t1", d.topicLinks().stream().
                filter(t -> "lnk1".equals(t.linkName())).findFirst().orElseThrow().physicalName());
            assertTrue(d.users().contains("User:alice"));
            assertTrue(d.consumerGroups().contains("g1"));

            ControllerResult<org.apache.kafka.common.message.DeleteVirtualClustersResponseData> del =
                f.virtualClusters.deleteVirtualClusters(ctx(ApiKeys.DELETE_VIRTUAL_CLUSTERS),
                    new DeleteVirtualClustersRequestData().
                        setVirtualClusters(new DeleteVirtualClustersRequestData.DeletableVirtualClusterCollection(
                            List.of(new DeleteVirtualClustersRequestData.DeletableVirtualCluster().setName("vc1")).iterator())));
            assertTrue(del.response().responses().find("vc1").errorCode() != Errors.NONE.code());

            AlterVirtualClustersRequestData.AlterableVirtualCluster avcRm =
                new AlterVirtualClustersRequestData.AlterableVirtualCluster().setName("vc1");
            avcRm.resources().add(new AlterVirtualClustersRequestData.VirtualClusterResourceChange().
                setResourceType(VirtualClusterControlManager.RESOURCE_TOPIC_LINK).
                setResourceOperation(VirtualClusterControlManager.OP_REMOVE).
                setResourceName("lnk1"));
            avcRm.resources().add(new AlterVirtualClustersRequestData.VirtualClusterResourceChange().
                setResourceType(VirtualClusterControlManager.RESOURCE_USER).
                setResourceOperation(VirtualClusterControlManager.OP_REMOVE).
                setResourceName("User:alice"));
            avcRm.resources().add(new AlterVirtualClustersRequestData.VirtualClusterResourceChange().
                setResourceType(VirtualClusterControlManager.RESOURCE_GROUP).
                setResourceOperation(VirtualClusterControlManager.OP_REMOVE).
                setResourceName("g1"));
            AlterVirtualClustersRequestData.AlterableVirtualClusterCollection rmColl =
                new AlterVirtualClustersRequestData.AlterableVirtualClusterCollection();
            rmColl.add(avcRm);
            ControllerResult<org.apache.kafka.common.message.AlterVirtualClustersResponseData> rm =
                f.virtualClusters.alterVirtualClusters(ctx(ApiKeys.ALTER_VIRTUAL_CLUSTERS),
                    new AlterVirtualClustersRequestData().setVirtualClusters(rmColl));
            f.replay(rm.records());

            ControllerResult<org.apache.kafka.common.message.DeleteVirtualClustersResponseData> del2 =
                f.virtualClusters.deleteVirtualClusters(ctx(ApiKeys.DELETE_VIRTUAL_CLUSTERS),
                    new DeleteVirtualClustersRequestData().
                        setVirtualClusters(new DeleteVirtualClustersRequestData.DeletableVirtualClusterCollection(
                            List.of(new DeleteVirtualClustersRequestData.DeletableVirtualCluster().setName("vc1")).iterator())));
            assertEquals(Errors.NONE, Errors.forCode(del2.response().responses().find("vc1").errorCode()));
            f.replay(del2.records());
        }
    }
}
