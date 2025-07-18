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
package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;


public class ShareCoordinatorRecordHelpers {
    public static CoordinatorRecord newShareSnapshotRecord(String groupId, Uuid topicId, int partitionId, ShareGroupOffset offsetData) {
        return CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(groupId)
                .setTopicId(topicId)
                .setPartition(partitionId),
            new ApiMessageAndVersion(new ShareSnapshotValue()
                .setSnapshotEpoch(offsetData.snapshotEpoch())
                .setStateEpoch(offsetData.stateEpoch())
                .setLeaderEpoch(offsetData.leaderEpoch())
                .setStartOffset(offsetData.startOffset())
                .setStateBatches(offsetData.stateBatches().stream()
                    .map(batch -> new ShareSnapshotValue.StateBatch()
                        .setFirstOffset(batch.firstOffset())
                        .setLastOffset(batch.lastOffset())
                        .setDeliveryCount(batch.deliveryCount())
                        .setDeliveryState(batch.deliveryState()))
                    .toList())
                .setCreateTimestamp(offsetData.createTimestamp())
                .setWriteTimestamp(offsetData.writeTimestamp()),
                (short) 0
            )
        );
    }

    public static CoordinatorRecord newShareUpdateRecord(String groupId, Uuid topicId, int partitionId, ShareGroupOffset offsetData) {
        return CoordinatorRecord.record(
            new ShareUpdateKey()
                .setGroupId(groupId)
                .setTopicId(topicId)
                .setPartition(partitionId),
            new ApiMessageAndVersion(new ShareUpdateValue()
                .setSnapshotEpoch(offsetData.snapshotEpoch())
                .setLeaderEpoch(offsetData.leaderEpoch())
                .setStartOffset(offsetData.startOffset())
                .setStateBatches(offsetData.stateBatches().stream()
                    .map(batch -> new ShareUpdateValue.StateBatch()
                        .setFirstOffset(batch.firstOffset())
                        .setLastOffset(batch.lastOffset())
                        .setDeliveryCount(batch.deliveryCount())
                        .setDeliveryState(batch.deliveryState()))
                    .toList()),
                (short) 0
            )
        );
    }

    public static CoordinatorRecord newShareStateTombstoneRecord(String groupId, Uuid topicId, int partitionId) {
        // Always generate share snapshot type record for tombstone.
        return CoordinatorRecord.tombstone(
            new ShareSnapshotKey()
                .setGroupId(groupId)
                .setTopicId(topicId)
                .setPartition(partitionId)
        );
    }
}
