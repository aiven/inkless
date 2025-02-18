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
package kafka.network;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The produce response queue for a connection.
 * <p>Produce requests are conditionally muted, meaning many request may be processed at the same time.
 * But responses need to be sent back in the same order as received, so we need to keep track of the order.
 */
class ProduceSendQueue {

    // CorrelationId -> SendResponse
    // keeps the same order as requests are received.
    // To be processed by a single thread, so no need for synchronization.
    private final Map<Integer, RequestChannel.SendResponse> inflight = new LinkedHashMap<>();

    ProduceSendQueue() {
    }

    void prepare(final Integer correlationId) {
        inflight.put(correlationId, null);
    }

    void update(final RequestChannel.SendResponse response) {
        final int correlationId = response.request().context().correlationId();
        final RequestChannel.SendResponse previous = inflight.put(correlationId, response);
        if (previous != null) {
            throw new IllegalStateException("Unexpected response for connection " + correlationId);
        }
    }

    boolean nextReady() {
        final Iterator<Integer> iterator = inflight.keySet().iterator();
        if (!iterator.hasNext()) return false;
        final RequestChannel.SendResponse peeked = inflight.get(iterator.next());
        return peeked != null;
    }

    RequestChannel.SendResponse take() {
        if (!nextReady()) {
            throw new IllegalStateException();
        }
        final Integer next = inflight.keySet().iterator().next();
        return inflight.remove(next);
    }
}