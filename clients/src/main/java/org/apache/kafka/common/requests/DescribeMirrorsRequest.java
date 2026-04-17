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

import org.apache.kafka.common.message.DescribeMirrorsRequestData;
import org.apache.kafka.common.message.DescribeMirrorsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Collections;

/**
 * Possible error codes:
 *
 * CLUSTER_AUTHORIZATION_FAILED (31)
 */
public class DescribeMirrorsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DescribeMirrorsRequest> {

        private final DescribeMirrorsRequestData data;

        public Builder(DescribeMirrorsRequestData data) {
            super(ApiKeys.DESCRIBE_MIRRORS);
            this.data = data;
        }

        @Override
        public DescribeMirrorsRequest build(short version) {
            return new DescribeMirrorsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeMirrorsRequestData data;

    public DescribeMirrorsRequest(DescribeMirrorsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_MIRRORS, version);
        this.data = data;
    }

    @Override
    public DescribeMirrorsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        DescribeMirrorsResponseData describeMirrorsResponseData = new DescribeMirrorsResponseData()
            .setMirrors(Collections.emptyList())
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(Errors.forException(e).code());

        return new DescribeMirrorsResponse(describeMirrorsResponseData);
    }

    public static DescribeMirrorsRequest parse(Readable readable, short version) {
        return new DescribeMirrorsRequest(new DescribeMirrorsRequestData(readable, version), version);
    }

    @Override
    public DescribeMirrorsRequestData data() {
        return data;
    }
}
