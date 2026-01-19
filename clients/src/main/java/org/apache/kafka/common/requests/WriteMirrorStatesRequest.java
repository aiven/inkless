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

import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.Set;

public class WriteMirrorStatesRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<WriteMirrorStatesRequest> {

        private final WriteMirrorStatesRequestData data;

        public Builder(WriteMirrorStatesRequestData data) {
            super(ApiKeys.WRITE_MIRROR_STATES);
            this.data = data;
        }

        public Builder(Set<String> topics) {
            super(ApiKeys.WRITE_MIRROR_STATES, ApiKeys.WRITE_MIRROR_STATES.oldestVersion(),
                    ApiKeys.WRITE_MIRROR_STATES.latestVersion());
            WriteMirrorStatesRequestData data = new WriteMirrorStatesRequestData();
            data.setTopics(new ArrayList<>(topics));
            this.data = data;
        }

        @Override
        public WriteMirrorStatesRequest build(short version) {
            return new WriteMirrorStatesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final WriteMirrorStatesRequestData data;

    public WriteMirrorStatesRequest(WriteMirrorStatesRequestData data, short version) {
        super(ApiKeys.WRITE_MIRROR_STATES, version);
        this.data = data;
    }

    @Override
    public WriteMirrorStatesRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        WriteMirrorStatesResponseData responseData = new WriteMirrorStatesResponseData();
        responseData.setErrorCode(error.code());

        return new WriteMirrorStatesResponse(responseData);
    }

    public static WriteMirrorStatesRequest parse(Readable readable, short version) {
        return new WriteMirrorStatesRequest(
                new WriteMirrorStatesRequestData(readable, version),
                version
        );
    }

}
