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

import org.apache.kafka.common.message.CreateVirtualClustersRequestData;
import org.apache.kafka.common.message.CreateVirtualClustersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class CreateVirtualClustersRequest extends AbstractRequest {

    private final CreateVirtualClustersRequestData data;

    public CreateVirtualClustersRequest(CreateVirtualClustersRequestData data, short apiVersion) {
        super(ApiKeys.CREATE_VIRTUAL_CLUSTERS, apiVersion);
        this.data = data;
    }

    @Override
    public CreateVirtualClustersRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        CreateVirtualClustersResponseData response = new CreateVirtualClustersResponseData()
            .setThrottleTimeMs(throttleTimeMs);
        data.virtualClusters().forEach(vc ->
            response.virtualClusters().add(new CreateVirtualClustersResponseData.CreatableVirtualClustersResult()
                .setName(vc.name())
                .setErrorCode(error.code())
                .setErrorMessage(error.message())));
        return new CreateVirtualClustersResponse(response);
    }

    public static CreateVirtualClustersRequest parse(Readable readable, short version) {
        return new CreateVirtualClustersRequest(new CreateVirtualClustersRequestData(readable, version), version);
    }

    public static class Builder extends AbstractRequest.Builder<CreateVirtualClustersRequest> {

        private final CreateVirtualClustersRequestData data;

        public Builder(CreateVirtualClustersRequestData data) {
            super(ApiKeys.CREATE_VIRTUAL_CLUSTERS,
                ApiKeys.CREATE_VIRTUAL_CLUSTERS.oldestVersion(),
                ApiKeys.CREATE_VIRTUAL_CLUSTERS.latestVersion());
            this.data = data;
        }

        @Override
        public CreateVirtualClustersRequest build(short version) {
            return new CreateVirtualClustersRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
