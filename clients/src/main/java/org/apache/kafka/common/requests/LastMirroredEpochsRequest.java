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

import org.apache.kafka.common.message.LastMirroredEpochsRequestData;
import org.apache.kafka.common.message.LastMirroredEpochsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class LastMirroredEpochsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LastMirroredEpochsRequest> {

        private final LastMirroredEpochsRequestData data;

        public Builder(LastMirroredEpochsRequestData data) {
            super(ApiKeys.LAST_MIRRORED_EPOCHS);
            this.data = data;
        }

        @Override
        public LastMirroredEpochsRequest build(short version) {
            return new LastMirroredEpochsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LastMirroredEpochsRequestData data;

    public LastMirroredEpochsRequest(LastMirroredEpochsRequestData data, short version) {
        super(ApiKeys.LAST_MIRRORED_EPOCHS, version);
        this.data = data;
    }

    @Override
    public LastMirroredEpochsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        LastMirroredEpochsResponseData responseData = new LastMirroredEpochsResponseData();
        responseData.setErrorCode(error.code());

        return new LastMirroredEpochsResponse(responseData);
    }

    public static LastMirroredEpochsRequest parse(Readable readable, short version) {
        return new LastMirroredEpochsRequest(
                new LastMirroredEpochsRequestData(readable, version),
                version
        );
    }
}
