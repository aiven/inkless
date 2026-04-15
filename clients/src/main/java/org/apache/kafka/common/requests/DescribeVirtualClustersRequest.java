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

import org.apache.kafka.common.message.DescribeVirtualClustersRequestData;
import org.apache.kafka.common.message.DescribeVirtualClustersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class DescribeVirtualClustersRequest extends AbstractRequest {

    private final DescribeVirtualClustersRequestData data;

    public DescribeVirtualClustersRequest(DescribeVirtualClustersRequestData data, short apiVersion) {
        super(ApiKeys.DESCRIBE_VIRTUAL_CLUSTERS, apiVersion);
        this.data = data;
    }

    @Override
    public DescribeVirtualClustersRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DescribeVirtualClustersResponseData response = new DescribeVirtualClustersResponseData()
            .setThrottleTimeMs(throttleTimeMs);
        data.virtualClusters().forEach(vc ->
            response.virtualClusters().add(new DescribeVirtualClustersResponseData.DescribedVirtualCluster()
                .setName(vc.name())
                .setErrorCode(error.code())));
        return new DescribeVirtualClustersResponse(response);
    }

    public static DescribeVirtualClustersRequest parse(Readable readable, short version) {
        return new DescribeVirtualClustersRequest(new DescribeVirtualClustersRequestData(readable, version), version);
    }

    public static class Builder extends AbstractRequest.Builder<DescribeVirtualClustersRequest> {

        private final DescribeVirtualClustersRequestData data;

        public Builder(DescribeVirtualClustersRequestData data) {
            super(ApiKeys.DESCRIBE_VIRTUAL_CLUSTERS,
                ApiKeys.DESCRIBE_VIRTUAL_CLUSTERS.oldestVersion(),
                ApiKeys.DESCRIBE_VIRTUAL_CLUSTERS.latestVersion());
            this.data = data;
        }

        @Override
        public DescribeVirtualClustersRequest build(short version) {
            return new DescribeVirtualClustersRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
