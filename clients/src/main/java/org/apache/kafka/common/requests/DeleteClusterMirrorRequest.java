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

import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.DeleteClusterMirrorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class DeleteClusterMirrorRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DeleteClusterMirrorRequest> {

        private final DeleteClusterMirrorRequestData data;

        public Builder(DeleteClusterMirrorRequestData data) {
            super(ApiKeys.DELETE_CLUSTER_MIRROR);
            this.data = data;
        }

        public Builder(String mirrorName) {
            this(mirrorName, -1);
        }

        public Builder(String mirrorName, long brokerMetadataOffset) {
            super(ApiKeys.DELETE_CLUSTER_MIRROR, ApiKeys.DELETE_CLUSTER_MIRROR.oldestVersion(),
                  ApiKeys.DELETE_CLUSTER_MIRROR.latestVersion());
            this.data = new DeleteClusterMirrorRequestData()
                    .setMirrorName(mirrorName)
                    .setStateValidationOffset(brokerMetadataOffset);
        }

        @Override
        public DeleteClusterMirrorRequest build(short version) {
            return new DeleteClusterMirrorRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DeleteClusterMirrorRequestData data;

    public DeleteClusterMirrorRequest(DeleteClusterMirrorRequestData data, short version) {
        super(ApiKeys.DELETE_CLUSTER_MIRROR, version);
        this.data = data;
    }

    @Override
    public DeleteClusterMirrorRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DeleteClusterMirrorResponseData responseData = new DeleteClusterMirrorResponseData();
        responseData.setErrorCode(error.code());
        return new DeleteClusterMirrorResponse(responseData);
    }

    public static DeleteClusterMirrorRequest parse(Readable readable, short version) {
        return new DeleteClusterMirrorRequest(
                new DeleteClusterMirrorRequestData(readable, version),
                version
        );
    }
}
