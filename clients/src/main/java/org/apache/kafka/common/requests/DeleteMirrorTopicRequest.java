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

import org.apache.kafka.common.message.DeleteMirrorTopicRequestData;
import org.apache.kafka.common.message.DeleteMirrorTopicResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Set;

public class DeleteMirrorTopicRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DeleteMirrorTopicRequest> {

        private final DeleteMirrorTopicRequestData data;

        public Builder(DeleteMirrorTopicRequestData data) {
            super(ApiKeys.DELETE_MIRROR_TOPIC);
            this.data = data;
        }

        public Builder(String name, Set<String> topics) {
            super(ApiKeys.DELETE_MIRROR_TOPIC, ApiKeys.DELETE_MIRROR_TOPIC.oldestVersion(),
                    ApiKeys.DELETE_MIRROR_TOPIC.latestVersion());
            DeleteMirrorTopicRequestData data = new DeleteMirrorTopicRequestData();
            data.setClusterLink(name);
            topics.forEach(topic -> data.topics().add(new DeleteMirrorTopicRequestData.TopicState().setName(topic)));
            this.data = data;
        }

        @Override
        public DeleteMirrorTopicRequest build(short version) {
            return new DeleteMirrorTopicRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DeleteMirrorTopicRequestData data;

    public DeleteMirrorTopicRequest(DeleteMirrorTopicRequestData data, short version) {
        super(ApiKeys.DELETE_MIRROR_TOPIC, version);
        this.data = data;
    }

    @Override
    public DeleteMirrorTopicRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DeleteMirrorTopicResponseData responseData = new DeleteMirrorTopicResponseData();
        responseData.setErrorCode(error.code());

        return new DeleteMirrorTopicResponse(responseData);
    }

    public static DeleteMirrorTopicRequest parse(Readable readable, short version) {
        return new DeleteMirrorTopicRequest(
                new DeleteMirrorTopicRequestData(readable, version),
                version
        );
    }

}
