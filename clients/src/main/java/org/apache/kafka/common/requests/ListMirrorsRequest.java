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

import org.apache.kafka.common.message.ListMirrorsRequestData;
import org.apache.kafka.common.message.ListMirrorsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Collections;

/**
 * Possible error codes:
 *
 * COORDINATOR_LOAD_IN_PROGRESS (14)
 * COORDINATOR_NOT_AVAILABLE (15)
 * AUTHORIZATION_FAILED (29)
 */
public class ListMirrorsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ListMirrorsRequest> {

        private final ListMirrorsRequestData data;

        public Builder(ListMirrorsRequestData data) {
            super(ApiKeys.LIST_MIRRORS);
            this.data = data;
        }

        @Override
        public ListMirrorsRequest build(short version) {
            return new ListMirrorsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListMirrorsRequestData data;

    public ListMirrorsRequest(ListMirrorsRequestData data, short version) {
        super(ApiKeys.LIST_MIRRORS, version);
        this.data = data;
    }

    @Override
    public ListMirrorsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ListMirrorsResponseData listMirrorsResponseData = new ListMirrorsResponseData()
            .setMirrors(Collections.emptyList())
            .setErrorCode(Errors.forException(e).code())
            .setThrottleTimeMs(throttleTimeMs);
        return new ListMirrorsResponse(listMirrorsResponseData);
    }

    public static ListMirrorsRequest parse(Readable readable, short version) {
        return new ListMirrorsRequest(new ListMirrorsRequestData(readable, version), version);
    }

    @Override
    public ListMirrorsRequestData data() {
        return data;
    }
}
