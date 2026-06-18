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

import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.PauseMirrorTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Set;

public class PauseMirrorTopicsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<PauseMirrorTopicsRequest> {

        private final PauseMirrorTopicsRequestData data;

        public Builder(PauseMirrorTopicsRequestData data) {
            super(ApiKeys.PAUSE_MIRROR_TOPICS);
            this.data = data;
        }

        public Builder(String mirrorName, Set<String> topics) {
            super(ApiKeys.PAUSE_MIRROR_TOPICS, ApiKeys.PAUSE_MIRROR_TOPICS.oldestVersion(),
                    ApiKeys.PAUSE_MIRROR_TOPICS.latestVersion());
            PauseMirrorTopicsRequestData data = new PauseMirrorTopicsRequestData();
            data.setMirrorName(mirrorName);
            topics.forEach(topic -> data.topics().add(new PauseMirrorTopicsRequestData.TopicMetadata().setTopicName(topic)));
            this.data = data;
        }

        @Override
        public PauseMirrorTopicsRequest build(short version) {
            return new PauseMirrorTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final PauseMirrorTopicsRequestData data;

    public PauseMirrorTopicsRequest(PauseMirrorTopicsRequestData data, short version) {
        super(ApiKeys.PAUSE_MIRROR_TOPICS, version);
        this.data = data;
    }

    @Override
    public PauseMirrorTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        PauseMirrorTopicsResponseData responseData = new PauseMirrorTopicsResponseData();
        responseData.setErrorCode(error.code());
        return new PauseMirrorTopicsResponse(responseData);
    }

    public static PauseMirrorTopicsRequest parse(Readable readable, short version) {
        return new PauseMirrorTopicsRequest(
                new PauseMirrorTopicsRequestData(readable, version),
                version
        );
    }
}
