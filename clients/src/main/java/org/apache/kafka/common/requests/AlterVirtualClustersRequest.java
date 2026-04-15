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

import org.apache.kafka.common.message.AlterVirtualClustersRequestData;
import org.apache.kafka.common.message.AlterVirtualClustersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class AlterVirtualClustersRequest extends AbstractRequest {

    private final AlterVirtualClustersRequestData data;

    public AlterVirtualClustersRequest(AlterVirtualClustersRequestData data, short apiVersion) {
        super(ApiKeys.ALTER_VIRTUAL_CLUSTERS, apiVersion);
        this.data = data;
    }

    @Override
    public AlterVirtualClustersRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        AlterVirtualClustersResponseData response = new AlterVirtualClustersResponseData()
            .setThrottleTimeMs(throttleTimeMs);
        for (AlterVirtualClustersRequestData.AlterableVirtualCluster vc : data.virtualClusters()) {
            AlterVirtualClustersResponseData.AlterableVirtualClusterResult vcResult =
                new AlterVirtualClustersResponseData.AlterableVirtualClusterResult().setName(vc.name());
            for (AlterVirtualClustersRequestData.VirtualClusterResourceChange res : vc.resources()) {
                vcResult.resources().add(new AlterVirtualClustersResponseData.VirtualClusterResourceResult()
                    .setResourceType(res.resourceType())
                    .setResourceOperation(res.resourceOperation())
                    .setResourceName(res.resourceName())
                    .setErrorCode(error.code())
                    .setErrorMessage(error.message()));
            }
            response.virtualClusters().add(vcResult);
        }
        return new AlterVirtualClustersResponse(response);
    }

    public static AlterVirtualClustersRequest parse(Readable readable, short version) {
        return new AlterVirtualClustersRequest(new AlterVirtualClustersRequestData(readable, version), version);
    }

    public static class Builder extends AbstractRequest.Builder<AlterVirtualClustersRequest> {

        private final AlterVirtualClustersRequestData data;

        public Builder(AlterVirtualClustersRequestData data) {
            super(ApiKeys.ALTER_VIRTUAL_CLUSTERS,
                ApiKeys.ALTER_VIRTUAL_CLUSTERS.oldestVersion(),
                ApiKeys.ALTER_VIRTUAL_CLUSTERS.latestVersion());
            this.data = data;
        }

        @Override
        public AlterVirtualClustersRequest build(short version) {
            return new AlterVirtualClustersRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
