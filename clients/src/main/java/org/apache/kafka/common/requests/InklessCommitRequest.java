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

import org.apache.kafka.common.message.InklessCommitRequestData;
import org.apache.kafka.common.message.InklessCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class InklessCommitRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<InklessCommitRequest> {
        public final InklessCommitRequestData data;

        public Builder(InklessCommitRequestData data) {
            super(ApiKeys.INKLESS_COMMIT);
            this.data = data;
        }

        @Override
        public InklessCommitRequest build(short version) {
            return new InklessCommitRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final InklessCommitRequestData data;

    private InklessCommitRequest(InklessCommitRequestData data, short version) {
        super(ApiKeys.INIT_PRODUCER_ID, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        InklessCommitResponseData response = new InklessCommitResponseData();
        return new InklessCommitResponse(response);
    }

    public static InklessCommitRequest parse(Readable readable, short version) {
        return new InklessCommitRequest(new InklessCommitRequestData(readable, version), version);
    }

    @Override
    public InklessCommitRequestData data() {
        return data;
    }
}
