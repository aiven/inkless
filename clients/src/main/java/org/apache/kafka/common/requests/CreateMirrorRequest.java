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

import org.apache.kafka.common.message.CreateMirrorRequestData;
import org.apache.kafka.common.message.CreateMirrorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Map;

public class CreateMirrorRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<CreateMirrorRequest> {

        private final CreateMirrorRequestData data;

        public Builder(CreateMirrorRequestData data) {
            super(ApiKeys.CREATE_MIRROR);
            this.data = data;
        }

        public Builder(String name, Map<String, String> configs) {
            super(ApiKeys.CREATE_MIRROR, ApiKeys.CREATE_MIRROR.oldestVersion(),
                  ApiKeys.CREATE_MIRROR.latestVersion());
            CreateMirrorRequestData data = new CreateMirrorRequestData();
            data.setMirrorName(name);
            CreateMirrorRequestData.MirrorConfigCollection configsCollection = new CreateMirrorRequestData.MirrorConfigCollection();
            configs.forEach((key, value) -> {
                configsCollection.add(new CreateMirrorRequestData.MirrorConfig().setName(key).setValue(value));
            });
            data.setConfig(configsCollection);

            this.data = data;
        }

        @Override
        public CreateMirrorRequest build(short version) {
            return new CreateMirrorRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final CreateMirrorRequestData data;

    public CreateMirrorRequest(CreateMirrorRequestData data, short version) {
        super(ApiKeys.CREATE_MIRROR, version);
        this.data = data;
    }

    @Override
    public CreateMirrorRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        CreateMirrorResponseData responseData = new CreateMirrorResponseData();
        responseData.setErrorCode(error.code());
        return new CreateMirrorResponse(responseData);
    }

    public static CreateMirrorRequest parse(Readable readable, short version) {
        return new CreateMirrorRequest(
                new CreateMirrorRequestData(readable, version),
                version
        );
    }
}
