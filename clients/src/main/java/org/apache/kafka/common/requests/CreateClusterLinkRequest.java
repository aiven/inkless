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

import org.apache.kafka.common.message.CreateClusterLinkRequestData;
import org.apache.kafka.common.message.CreateClusterLinkResponseData;
import org.apache.kafka.common.message.GetReplicaLogInfoRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateClusterLinkRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<CreateClusterLinkRequest> {

        private final CreateClusterLinkRequestData data;

        public Builder(CreateClusterLinkRequestData data) {
            super(ApiKeys.CREATE_CLUSTER_LINK);
            this.data = data;
        }

        public Builder(String name, Map<String, String> configs) {
            super(ApiKeys.CREATE_CLUSTER_LINK, ApiKeys.CREATE_CLUSTER_LINK.oldestVersion(),
                  ApiKeys.CREATE_CLUSTER_LINK.latestVersion());
            CreateClusterLinkRequestData data = new CreateClusterLinkRequestData();
            data.setClusterLinkName(name);
            CreateClusterLinkRequestData.ClusterLinkConfigsCollection configsCollection = new CreateClusterLinkRequestData.ClusterLinkConfigsCollection();
            configs.forEach((key, value) -> {
                configsCollection.add(new CreateClusterLinkRequestData.ClusterLinkConfigs().setName(key).setValue(value));
            });
            data.setConfigs(configsCollection);

            this.data = data;
        }

        @Override
        public CreateClusterLinkRequest build(short version) {
            return new CreateClusterLinkRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final CreateClusterLinkRequestData data;

    public CreateClusterLinkRequest(CreateClusterLinkRequestData data, short version) {
        super(ApiKeys.CREATE_CLUSTER_LINK, version);
        this.data = data;
    }

    @Override
    public CreateClusterLinkRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        CreateClusterLinkResponseData responseData = new CreateClusterLinkResponseData();
        responseData.setErrorCode(error.code());

        return new CreateClusterLinkResponse(responseData);
    }

    public static CreateClusterLinkRequest parse(Readable readable, short version) {
        return new CreateClusterLinkRequest(
                new CreateClusterLinkRequestData(readable, version),
                version
        );
    }

}
