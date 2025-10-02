/*
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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DisklessCommitRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class DisklessCommitRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DisklessCommitRequest> {
        private final DisklessCommitRequestData data;

        public Builder(DisklessCommitRequestData data) {
            super(ApiKeys.DISKLESS_COMMIT);
            this.data = data;
        }

        @Override
        public DisklessCommitRequest build(short version) {
            return new DisklessCommitRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DisklessCommitRequestData data;

    public DisklessCommitRequest(DisklessCommitRequestData data, short version) {
        super(ApiKeys.DISKLESS_COMMIT, version);
        this.data = data;
    }

    @Override
    public DisklessCommitRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        org.apache.kafka.common.message.DisklessCommitResponseData responseData = 
            new org.apache.kafka.common.message.DisklessCommitResponseData()
                .setThrottleTimeMs(throttleTimeMs);
        
        org.apache.kafka.common.message.DisklessCommitResponseData.TopicDisklessCommitResponseCollection topicResponses = 
            new org.apache.kafka.common.message.DisklessCommitResponseData.TopicDisklessCommitResponseCollection();
        
        data.topics().forEach(topicData -> {
            org.apache.kafka.common.message.DisklessCommitResponseData.TopicDisklessCommitResponse topicResponse = 
                new org.apache.kafka.common.message.DisklessCommitResponseData.TopicDisklessCommitResponse()
                    .setTopicId(topicData.topicId());
            
            java.util.List<org.apache.kafka.common.message.DisklessCommitResponseData.PartitionDisklessCommitResponse> partitionResponses = 
                new java.util.ArrayList<>();
            
            topicData.partitions().forEach(partitionData -> {
                partitionResponses.add(new org.apache.kafka.common.message.DisklessCommitResponseData.PartitionDisklessCommitResponse()
                    .setPartition(partitionData.partition())
                    .setErrorCode(Errors.forException(e).code())
                    .setErrorMessage(e.getMessage()));
            });
            
            topicResponse.setPartitionResponses(partitionResponses);
            topicResponses.add(topicResponse);
        });
        
        responseData.setResponses(topicResponses);
        return new DisklessCommitResponse(responseData);
    }

    public static DisklessCommitRequest parse(Readable readable, short version) {
        return new DisklessCommitRequest(new DisklessCommitRequestData(readable, version), version);
    }
}
