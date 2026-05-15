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

import org.apache.kafka.common.message.CreateClusterMirrorRequestData;
import org.apache.kafka.common.message.CreateClusterMirrorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Map;

public class CreateClusterMirrorRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<CreateClusterMirrorRequest> {

        private final CreateClusterMirrorRequestData data;

        public Builder(CreateClusterMirrorRequestData data) {
            super(ApiKeys.CREATE_CLUSTER_MIRROR);
            this.data = data;
        }

        public Builder(String name, Map<String, String> configs) {
            super(ApiKeys.CREATE_CLUSTER_MIRROR, ApiKeys.CREATE_CLUSTER_MIRROR.oldestVersion(),
                  ApiKeys.CREATE_CLUSTER_MIRROR.latestVersion());
            CreateClusterMirrorRequestData data = new CreateClusterMirrorRequestData();
            data.setMirrorName(name);
            CreateClusterMirrorRequestData.ClusterMirrorConfigCollection configsCollection = new CreateClusterMirrorRequestData.ClusterMirrorConfigCollection();
            configs.forEach((key, value) -> {
                configsCollection.add(new CreateClusterMirrorRequestData.ClusterMirrorConfig().setName(key).setValue(value));
            });
            data.setConfig(configsCollection);

            this.data = data;
        }

        @Override
        public CreateClusterMirrorRequest build(short version) {
            return new CreateClusterMirrorRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final CreateClusterMirrorRequestData data;

    public CreateClusterMirrorRequest(CreateClusterMirrorRequestData data, short version) {
        super(ApiKeys.CREATE_CLUSTER_MIRROR, version);
        this.data = data;
    }

    @Override
    public CreateClusterMirrorRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        CreateClusterMirrorResponseData responseData = new CreateClusterMirrorResponseData();
        responseData.setErrorCode(error.code());
        return new CreateClusterMirrorResponse(responseData);
    }

    public static CreateClusterMirrorRequest parse(Readable readable, short version) {
        return new CreateClusterMirrorRequest(
                new CreateClusterMirrorRequestData(readable, version),
                version
        );
    }
}
