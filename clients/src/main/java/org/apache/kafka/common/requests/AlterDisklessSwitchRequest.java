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

import org.apache.kafka.common.message.AlterDisklessSwitchRequestData;
import org.apache.kafka.common.message.AlterDisklessSwitchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class AlterDisklessSwitchRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<AlterDisklessSwitchRequest> {
        private final AlterDisklessSwitchRequestData data;

        public Builder(AlterDisklessSwitchRequestData data) {
            super(ApiKeys.ALTER_DISKLESS_SWITCH);
            this.data = data;
        }

        @Override
        public AlterDisklessSwitchRequest build(short version) {
            return new AlterDisklessSwitchRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AlterDisklessSwitchRequestData data;

    public AlterDisklessSwitchRequest(AlterDisklessSwitchRequestData data, short version) {
        super(ApiKeys.ALTER_DISKLESS_SWITCH, version);
        this.data = data;
    }

    @Override
    public AlterDisklessSwitchRequestData data() {
        return data;
    }

    @Override
    public AlterDisklessSwitchResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        return new AlterDisklessSwitchResponse(new AlterDisklessSwitchResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(apiError.error().code())
            .setErrorMessage(apiError.message()));
    }

    public static AlterDisklessSwitchRequest parse(Readable readable, short version) {
        return new AlterDisklessSwitchRequest(new AlterDisklessSwitchRequestData(readable, version), version);
    }
}
