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

import org.apache.kafka.common.message.DisklessCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class DisklessCommitResponse extends AbstractResponse {
    private final DisklessCommitResponseData data;

    public DisklessCommitResponse(DisklessCommitResponseData data) {
        super(ApiKeys.DISKLESS_COMMIT);
        this.data = data;
    }

    @Override
    public DisklessCommitResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public java.util.Map<org.apache.kafka.common.protocol.Errors, Integer> errorCounts() {
        java.util.Map<org.apache.kafka.common.protocol.Errors, Integer> errorCounts = new java.util.HashMap<>();
        data.responses().forEach(topicResponse -> 
            topicResponse.partitionResponses().forEach(partitionResponse -> {
                org.apache.kafka.common.protocol.Errors error = org.apache.kafka.common.protocol.Errors.forCode(partitionResponse.errorCode());
                errorCounts.put(error, errorCounts.getOrDefault(error, 0) + 1);
            })
        );
        return errorCounts;
    }

    public static DisklessCommitResponse parse(Readable readable, short version) {
        return new DisklessCommitResponse(new DisklessCommitResponseData(readable, version));
    }
}
