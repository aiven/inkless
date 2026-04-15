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

import org.apache.kafka.common.message.ListVirtualClustersRequestData;
import org.apache.kafka.common.message.ListVirtualClustersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ListVirtualClustersRequest extends AbstractRequest {

    private final ListVirtualClustersRequestData data;

    public ListVirtualClustersRequest(ListVirtualClustersRequestData data, short apiVersion) {
        super(ApiKeys.LIST_VIRTUAL_CLUSTERS, apiVersion);
        this.data = data;
    }

    @Override
    public ListVirtualClustersRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new ListVirtualClustersResponse(new ListVirtualClustersResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code()));
    }

    public static ListVirtualClustersRequest parse(Readable readable, short version) {
        return new ListVirtualClustersRequest(new ListVirtualClustersRequestData(readable, version), version);
    }

    public static class Builder extends AbstractRequest.Builder<ListVirtualClustersRequest> {

        private final ListVirtualClustersRequestData data;

        public Builder(ListVirtualClustersRequestData data) {
            super(ApiKeys.LIST_VIRTUAL_CLUSTERS,
                ApiKeys.LIST_VIRTUAL_CLUSTERS.oldestVersion(),
                ApiKeys.LIST_VIRTUAL_CLUSTERS.latestVersion());
            this.data = data;
        }

        @Override
        public ListVirtualClustersRequest build(short version) {
            return new ListVirtualClustersRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
