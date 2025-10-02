/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DisklessCommitOptions;
import org.apache.kafka.clients.admin.DisklessCommitResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.DisklessCommitRequestData;
import org.apache.kafka.common.message.DisklessCommitRequestData.TopicDisklessCommitData;
import org.apache.kafka.common.message.DisklessCommitRequestData.PartitionDisklessCommitData;
import org.apache.kafka.common.message.DisklessCommitRequestData.BatchDisklessCommitData;
import org.apache.kafka.common.message.DisklessCommitResponseData;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.DisklessCommitRequest;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
/**
 * A simple example that demonstrates how to use the AdminClient to send a DisklessCommitRequest.
 */
public class DisklessAPITester {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        final String topicName = "tx";

        Admin admin = Admin.create(props);

        final DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(List.of(topicName));
        try {
            deleteTopicsResult.all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
        }

        final Uuid topicId = admin.createTopics(List.of(new NewTopic(topicName, 2, (short) 1))).topicId(topicName).get();
        System.out.printf("Topic ID:%s%n", topicId);

        Thread.sleep(3000);

        BatchDisklessCommitData batch0 = new BatchDisklessCommitData()
            .setMagic(RecordBatch.CURRENT_MAGIC_VALUE)
            .setByteOffset(0)
            .setByteSize(1024)
            .setBaseOffset(0)
            .setLastOffset(9)
            .setTimestampType((short) TimestampType.LOG_APPEND_TIME.id)
            .setBatchMaxTimestamp(-1)
            .setProducerId(-1)
            .setProducerEpoch((short) -1)
            .setBaseSequence(-1)
            .setLastSequence(-1);
        BatchDisklessCommitData batch1 = new BatchDisklessCommitData()
            .setMagic(RecordBatch.CURRENT_MAGIC_VALUE)
            .setByteOffset(1024)
            .setByteSize(2048)
            .setBaseOffset(10)
            .setLastOffset(19)
            .setTimestampType((short) TimestampType.LOG_APPEND_TIME.id)
            .setBatchMaxTimestamp(-1)
            .setProducerId(-1)
            .setProducerEpoch((short) -1)
            .setBaseSequence(-1)
            .setLastSequence(-1);

        final PartitionDisklessCommitData partitionData = new PartitionDisklessCommitData()
            .setPartition(0)
            .setBatches(List.of(batch0, batch1));
        final TopicDisklessCommitData topicData = new TopicDisklessCommitData()
            .setTopicId(topicId)
            .setPartitions(List.of(partitionData));
        final DisklessCommitRequestData.TopicDisklessCommitDataCollection topicDataCollection =
            new DisklessCommitRequestData.TopicDisklessCommitDataCollection();
        topicDataCollection.add(topicData);
        final DisklessCommitRequestData commitRequestData = new DisklessCommitRequestData()
            .setObjectName("file1")
            .setTopics(topicDataCollection);
        final var response = admin.disklessCommit(commitRequestData).all().get();
        System.out.println(response);
    }
}
