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

import org.apache.kafka.common.message.InitDisklessLogRequestData;
import org.apache.kafka.common.message.InitDisklessLogResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class InitDisklessLogRequest extends AbstractRequest {

    private final InitDisklessLogRequestData data;

    public InitDisklessLogRequest(InitDisklessLogRequestData data, short apiVersion) {
        super(ApiKeys.INIT_DISKLESS_LOG, apiVersion);
        this.data = data;
    }

    @Override
    public InitDisklessLogRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new InitDisklessLogResponse(new InitDisklessLogResponseData()
            .setThrottleTimeMs(throttleTimeMs));
    }

    public static InitDisklessLogRequest parse(Readable readable, short version) {
        return new InitDisklessLogRequest(new InitDisklessLogRequestData(readable, version), version);
    }

    public static class Builder extends AbstractRequest.Builder<InitDisklessLogRequest> {

        private final InitDisklessLogRequestData data;

        public Builder(InitDisklessLogRequestData data) {
            super(
                ApiKeys.INIT_DISKLESS_LOG,
                ApiKeys.INIT_DISKLESS_LOG.oldestVersion(),
                ApiKeys.INIT_DISKLESS_LOG.latestVersion()
            );
            this.data = data;
        }

        @Override
        public InitDisklessLogRequest build(short version) {
            return new InitDisklessLogRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
