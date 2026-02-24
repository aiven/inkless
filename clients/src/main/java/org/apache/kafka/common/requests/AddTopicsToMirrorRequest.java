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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Map;

public class AddTopicsToMirrorRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<AddTopicsToMirrorRequest> {

        private final AddTopicsToMirrorRequestData data;

        public Builder(AddTopicsToMirrorRequestData data) {
            super(ApiKeys.ADD_TOPICS_TO_MIRROR);
            this.data = data;
        }

        public Builder(Map<String, String> topicToMirrorName) {
            super(ApiKeys.ADD_TOPICS_TO_MIRROR, ApiKeys.ADD_TOPICS_TO_MIRROR.oldestVersion(),
                    ApiKeys.ADD_TOPICS_TO_MIRROR.latestVersion());
            AddTopicsToMirrorRequestData data = new AddTopicsToMirrorRequestData();
            topicToMirrorName.forEach((topic, mirrorName) -> data.topics().add(new AddTopicsToMirrorRequestData.TopicData().setTopicName(topic).setMirrorName(mirrorName)));
            this.data = data;
        }

        @Override
        public AddTopicsToMirrorRequest build(short version) {
            return new AddTopicsToMirrorRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AddTopicsToMirrorRequestData data;

    public AddTopicsToMirrorRequest(AddTopicsToMirrorRequestData data, short version) {
        super(ApiKeys.ADD_TOPICS_TO_MIRROR, version);
        this.data = data;
    }

    @Override
    public AddTopicsToMirrorRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        AddTopicsToMirrorResponseData responseData = new AddTopicsToMirrorResponseData();
        responseData.setErrorCode(error.code());

        return new AddTopicsToMirrorResponse(responseData);
    }

    public static AddTopicsToMirrorRequest parse(Readable readable, short version) {
        return new AddTopicsToMirrorRequest(
                new AddTopicsToMirrorRequestData(readable, version),
                version
        );
    }

}
