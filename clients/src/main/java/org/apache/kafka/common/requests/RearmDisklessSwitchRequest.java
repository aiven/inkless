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

import org.apache.kafka.common.message.RearmDisklessSwitchRequestData;
import org.apache.kafka.common.message.RearmDisklessSwitchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class RearmDisklessSwitchRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<RearmDisklessSwitchRequest> {
        private final RearmDisklessSwitchRequestData data;

        public Builder(RearmDisklessSwitchRequestData data) {
            super(ApiKeys.REARM_DISKLESS_SWITCH);
            this.data = data;
        }

        @Override
        public RearmDisklessSwitchRequest build(short version) {
            return new RearmDisklessSwitchRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final RearmDisklessSwitchRequestData data;

    public RearmDisklessSwitchRequest(RearmDisklessSwitchRequestData data, short version) {
        super(ApiKeys.REARM_DISKLESS_SWITCH, version);
        this.data = data;
    }

    @Override
    public RearmDisklessSwitchRequestData data() {
        return data;
    }

    @Override
    public RearmDisklessSwitchResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        return new RearmDisklessSwitchResponse(new RearmDisklessSwitchResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(apiError.error().code())
            .setErrorMessage(apiError.message()));
    }

    public static RearmDisklessSwitchRequest parse(Readable readable, short version) {
        return new RearmDisklessSwitchRequest(new RearmDisklessSwitchRequestData(readable, version), version);
    }
}
