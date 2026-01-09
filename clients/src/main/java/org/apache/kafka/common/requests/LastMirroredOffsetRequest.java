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

import org.apache.kafka.common.message.AddTopicsToMirrorRequestData;
import org.apache.kafka.common.message.AddTopicsToMirrorResponseData;
import org.apache.kafka.common.message.LastMirroredOffsetRequestData;
import org.apache.kafka.common.message.LastMirroredOffsetResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class LastMirroredOffsetRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LastMirroredOffsetRequest> {

        private final LastMirroredOffsetRequestData data;

        public Builder(LastMirroredOffsetRequestData data) {
            super(ApiKeys.LAST_MIRRORED_OFFSET);
            this.data = data;
        }

        public Builder(Set<String> topics) {
            super(ApiKeys.LAST_MIRRORED_OFFSET, ApiKeys.LAST_MIRRORED_OFFSET.oldestVersion(),
                    ApiKeys.LAST_MIRRORED_OFFSET.latestVersion());
            LastMirroredOffsetRequestData data = new LastMirroredOffsetRequestData();
            data.setTopics(new ArrayList<>(topics));
            this.data = data;
        }

        @Override
        public LastMirroredOffsetRequest build(short version) {
            return new LastMirroredOffsetRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LastMirroredOffsetRequestData data;

    public LastMirroredOffsetRequest(LastMirroredOffsetRequestData data, short version) {
        super(ApiKeys.LAST_MIRRORED_OFFSET, version);
        this.data = data;
    }

    @Override
    public LastMirroredOffsetRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        LastMirroredOffsetResponseData responseData = new LastMirroredOffsetResponseData();
        responseData.setErrorCode(error.code());

        return new LastMirroredOffsetResponse(responseData);
    }

    public static LastMirroredOffsetRequest parse(Readable readable, short version) {
        return new LastMirroredOffsetRequest(
                new LastMirroredOffsetRequestData(readable, version),
                version
        );
    }

}
