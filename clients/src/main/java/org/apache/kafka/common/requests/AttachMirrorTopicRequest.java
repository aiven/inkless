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

import org.apache.kafka.common.message.AttachMirrorTopicRequestData;
import org.apache.kafka.common.message.AttachMirrorTopicResponseData;
import org.apache.kafka.common.message.DeleteMirrorTopicRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Map;

public class AttachMirrorTopicRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<AttachMirrorTopicRequest> {

        private final AttachMirrorTopicRequestData data;

        public Builder(AttachMirrorTopicRequestData data) {
            super(ApiKeys.ATTACH_MIRROR_TOPIC);
            this.data = data;
        }

        public Builder(Map<String, String> topicToMirrorName) {
            super(ApiKeys.ATTACH_MIRROR_TOPIC, ApiKeys.ATTACH_MIRROR_TOPIC.oldestVersion(),
                    ApiKeys.ATTACH_MIRROR_TOPIC.latestVersion());
            AttachMirrorTopicRequestData data = new AttachMirrorTopicRequestData();
            topicToMirrorName.forEach((topic, mirrorName) -> data.topics().add(new AttachMirrorTopicRequestData.TopicState().setName(topic).setMirrorName(mirrorName)));
            this.data = data;
        }

        @Override
        public AttachMirrorTopicRequest build(short version) {
            return new AttachMirrorTopicRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AttachMirrorTopicRequestData data;

    public AttachMirrorTopicRequest(AttachMirrorTopicRequestData data, short version) {
        super(ApiKeys.ATTACH_MIRROR_TOPIC, version);
        this.data = data;
    }

    @Override
    public AttachMirrorTopicRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        AttachMirrorTopicResponseData responseData = new AttachMirrorTopicResponseData();
        responseData.setErrorCode(error.code());

        return new AttachMirrorTopicResponse(responseData);
    }

    public static AttachMirrorTopicRequest parse(Readable readable, short version) {
        return new AttachMirrorTopicRequest(
                new AttachMirrorTopicRequestData(readable, version),
                version
        );
    }

}
