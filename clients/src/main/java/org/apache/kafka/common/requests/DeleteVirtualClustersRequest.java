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

import org.apache.kafka.common.message.DeleteVirtualClustersRequestData;
import org.apache.kafka.common.message.DeleteVirtualClustersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class DeleteVirtualClustersRequest extends AbstractRequest {

    private final DeleteVirtualClustersRequestData data;

    public DeleteVirtualClustersRequest(DeleteVirtualClustersRequestData data, short apiVersion) {
        super(ApiKeys.DELETE_VIRTUAL_CLUSTERS, apiVersion);
        this.data = data;
    }

    @Override
    public DeleteVirtualClustersRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DeleteVirtualClustersResponseData response = new DeleteVirtualClustersResponseData()
            .setThrottleTimeMs(throttleTimeMs);
        data.virtualClusters().forEach(vc ->
            response.responses().add(new DeleteVirtualClustersResponseData.DeletableVirtualClusterResult()
                .setName(vc.name())
                .setErrorCode(error.code())
                .setErrorMessage(error.message())));
        return new DeleteVirtualClustersResponse(response);
    }

    public static DeleteVirtualClustersRequest parse(Readable readable, short version) {
        return new DeleteVirtualClustersRequest(new DeleteVirtualClustersRequestData(readable, version), version);
    }

    public static class Builder extends AbstractRequest.Builder<DeleteVirtualClustersRequest> {

        private final DeleteVirtualClustersRequestData data;

        public Builder(DeleteVirtualClustersRequestData data) {
            super(ApiKeys.DELETE_VIRTUAL_CLUSTERS,
                ApiKeys.DELETE_VIRTUAL_CLUSTERS.oldestVersion(),
                ApiKeys.DELETE_VIRTUAL_CLUSTERS.latestVersion());
            this.data = data;
        }

        @Override
        public DeleteVirtualClustersRequest build(short version) {
            return new DeleteVirtualClustersRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
