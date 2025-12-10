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

import org.apache.kafka.common.message.RemoveTopicsFromMirrorRequestData;
import org.apache.kafka.common.message.RemoveTopicsFromMirrorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Set;

public class RemoveTopicsFromMirrorRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<RemoveTopicsFromMirrorRequest> {

        private final RemoveTopicsFromMirrorRequestData data;

        public Builder(RemoveTopicsFromMirrorRequestData data) {
            super(ApiKeys.REMOVE_TOPICS_FROM_MIRROR);
            this.data = data;
        }

        public Builder(String name, Set<String> topics) {
            super(ApiKeys.REMOVE_TOPICS_FROM_MIRROR, ApiKeys.REMOVE_TOPICS_FROM_MIRROR.oldestVersion(),
                    ApiKeys.REMOVE_TOPICS_FROM_MIRROR.latestVersion());
            RemoveTopicsFromMirrorRequestData data = new RemoveTopicsFromMirrorRequestData();
            data.setMirrorName(name);
            topics.forEach(topic -> data.topics().add(new RemoveTopicsFromMirrorRequestData.TopicState().setTopicName(topic)));
            this.data = data;
        }

        @Override
        public RemoveTopicsFromMirrorRequest build(short version) {
            return new RemoveTopicsFromMirrorRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final RemoveTopicsFromMirrorRequestData data;

    public RemoveTopicsFromMirrorRequest(RemoveTopicsFromMirrorRequestData data, short version) {
        super(ApiKeys.REMOVE_TOPICS_FROM_MIRROR, version);
        this.data = data;
    }

    @Override
    public RemoveTopicsFromMirrorRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        RemoveTopicsFromMirrorResponseData responseData = new RemoveTopicsFromMirrorResponseData();
        responseData.setErrorCode(error.code());

        return new RemoveTopicsFromMirrorResponse(responseData);
    }

    public static RemoveTopicsFromMirrorRequest parse(Readable readable, short version) {
        return new RemoveTopicsFromMirrorRequest(
                new RemoveTopicsFromMirrorRequestData(readable, version),
                version
        );
    }

}
