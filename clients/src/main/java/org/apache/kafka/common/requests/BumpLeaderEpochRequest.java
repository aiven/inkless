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

import org.apache.kafka.common.message.BumpLeaderEpochRequestData;
import org.apache.kafka.common.message.BumpLeaderEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Map;

public class BumpLeaderEpochRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<BumpLeaderEpochRequest> {

        private final BumpLeaderEpochRequestData data;

        public Builder(BumpLeaderEpochRequestData data) {
            super(ApiKeys.BUMP_LEADER_EPOCH);
            this.data = data;
        }

        public Builder(String name, Map<String, String> configs) {
            super(ApiKeys.BUMP_LEADER_EPOCH, ApiKeys.BUMP_LEADER_EPOCH.oldestVersion(),
                    ApiKeys.BUMP_LEADER_EPOCH.latestVersion());
            BumpLeaderEpochRequestData data = new BumpLeaderEpochRequestData();
            this.data = data;
        }

        @Override
        public BumpLeaderEpochRequest build(short version) {
            return new BumpLeaderEpochRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final BumpLeaderEpochRequestData data;

    public BumpLeaderEpochRequest(BumpLeaderEpochRequestData data, short version) {
        super(ApiKeys.BUMP_LEADER_EPOCH, version);
        this.data = data;
    }

    @Override
    public BumpLeaderEpochRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        BumpLeaderEpochResponseData responseData = new BumpLeaderEpochResponseData();
        responseData.setErrorCode(error.code());

        return new BumpLeaderEpochResponse(responseData);
    }

    public static BumpLeaderEpochRequest parse(Readable readable, short version) {
        return new BumpLeaderEpochRequest(
                new BumpLeaderEpochRequestData(readable, version),
                version
        );
    }

}
