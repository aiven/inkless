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

import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Set;

public class StartMirrorTopicsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<StartMirrorTopicsRequest> {

        private final StartMirrorTopicsRequestData data;

        public Builder(StartMirrorTopicsRequestData data) {
            super(ApiKeys.START_MIRROR_TOPICS);
            this.data = data;
        }

        public Builder(String mirrorName, Set<String> topics) {
            super(ApiKeys.START_MIRROR_TOPICS, ApiKeys.START_MIRROR_TOPICS.oldestVersion(),
                    ApiKeys.START_MIRROR_TOPICS.latestVersion());
            StartMirrorTopicsRequestData data = new StartMirrorTopicsRequestData();
            data.setMirrorName(mirrorName);
            topics.forEach(topic -> data.topics().add(new StartMirrorTopicsRequestData.TopicData().setTopicName(topic)));
            this.data = data;
        }

        @Override
        public StartMirrorTopicsRequest build(short version) {
            return new StartMirrorTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final StartMirrorTopicsRequestData data;

    public StartMirrorTopicsRequest(StartMirrorTopicsRequestData data, short version) {
        super(ApiKeys.START_MIRROR_TOPICS, version);
        this.data = data;
    }

    @Override
    public StartMirrorTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        StartMirrorTopicsResponseData responseData = new StartMirrorTopicsResponseData();
        responseData.setErrorCode(error.code());

        return new StartMirrorTopicsResponse(responseData);
    }

    public static StartMirrorTopicsRequest parse(Readable readable, short version) {
        return new StartMirrorTopicsRequest(
                new StartMirrorTopicsRequestData(readable, version),
                version
        );
    }

}
