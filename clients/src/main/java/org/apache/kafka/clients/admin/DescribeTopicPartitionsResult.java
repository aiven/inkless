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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;

/**
 * Raw protocol result for {@link Admin#describeTopicPartitions(java.util.Collection, DescribeTopicsOptions)}.
 * This exposes the raw protocol response data directly rather than a typed model.
 * The returned data is mutable and may contain embedded errors that callers must inspect.
 */
public class DescribeTopicPartitionsResult {

    private final KafkaFuture<DescribeTopicPartitionsResponseData> future;

    public DescribeTopicPartitionsResult(KafkaFuture<DescribeTopicPartitionsResponseData> future) {
        this.future = future;
    }

    /**
     * Returns the raw {@link DescribeTopicPartitionsResponseData} accumulated across all pagination rounds.
     * Topic-level and partition-level errors are checked during accumulation and will cause the future
     * to complete exceptionally. The returned data is mutable.
     */
    public KafkaFuture<DescribeTopicPartitionsResponseData> rawResponse() {
        return future;
    }
}
