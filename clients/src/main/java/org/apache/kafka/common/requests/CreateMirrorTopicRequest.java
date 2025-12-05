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

import org.apache.kafka.common.message.CreateMirrorTopicRequestData;
import org.apache.kafka.common.message.CreateMirrorTopicResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Map;

public class CreateMirrorTopicRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<CreateMirrorTopicRequest> {

        private final CreateMirrorTopicRequestData data;

        public Builder(CreateMirrorTopicRequestData data) {
            super(ApiKeys.CREATE_MIRROR_TOPIC);
            this.data = data;
        }

        public Builder(String name, Map<String, String> configs) {
            super(ApiKeys.CREATE_MIRROR_TOPIC, ApiKeys.CREATE_MIRROR_TOPIC.oldestVersion(),
                    ApiKeys.CREATE_MIRROR_TOPIC.latestVersion());
            CreateMirrorTopicRequestData data = new CreateMirrorTopicRequestData();
            this.data = data;
        }

        @Override
        public CreateMirrorTopicRequest build(short version) {
            return new CreateMirrorTopicRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final CreateMirrorTopicRequestData data;

    public CreateMirrorTopicRequest(CreateMirrorTopicRequestData data, short version) {
        super(ApiKeys.CREATE_MIRROR_TOPIC, version);
        this.data = data;
    }

    @Override
    public CreateMirrorTopicRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        CreateMirrorTopicResponseData responseData = new CreateMirrorTopicResponseData();
        responseData.setErrorCode(error.code());

        return new CreateMirrorTopicResponse(responseData);
    }

    public static CreateMirrorTopicRequest parse(Readable readable, short version) {
        return new CreateMirrorTopicRequest(
                new CreateMirrorTopicRequestData(readable, version),
                version
        );
    }

}
