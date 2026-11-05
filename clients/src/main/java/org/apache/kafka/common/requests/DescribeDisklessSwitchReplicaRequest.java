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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DescribeDisklessSwitchReplicaRequestData;
import org.apache.kafka.common.message.DescribeDisklessSwitchReplicaResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class DescribeDisklessSwitchReplicaRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DescribeDisklessSwitchReplicaRequest> {
        private final DescribeDisklessSwitchReplicaRequestData data;

        public Builder(DescribeDisklessSwitchReplicaRequestData data) {
            super(ApiKeys.DESCRIBE_DISKLESS_SWITCH_REPLICA);
            this.data = data;
        }

        @Override
        public DescribeDisklessSwitchReplicaRequest build(short version) {
            return new DescribeDisklessSwitchReplicaRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeDisklessSwitchReplicaRequestData data;

    public DescribeDisklessSwitchReplicaRequest(DescribeDisklessSwitchReplicaRequestData data, short version) {
        super(ApiKeys.DESCRIBE_DISKLESS_SWITCH_REPLICA, version);
        this.data = data;
    }

    @Override
    public DescribeDisklessSwitchReplicaRequestData data() {
        return data;
    }

    @Override
    public DescribeDisklessSwitchReplicaResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        return new DescribeDisklessSwitchReplicaResponse(new DescribeDisklessSwitchReplicaResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(apiError.error().code())
            .setErrorMessage(apiError.message()));
    }

    public static DescribeDisklessSwitchReplicaRequest parse(Readable readable, short version) {
        return new DescribeDisklessSwitchReplicaRequest(new DescribeDisklessSwitchReplicaRequestData(readable, version), version);
    }
}
