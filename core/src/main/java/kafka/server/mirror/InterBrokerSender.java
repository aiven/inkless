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
package kafka.server.mirror;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Queue-based sender for asynchronous inter-broker requests used by {@link MirrorMetadataManager}
 * to forward mirror state updates to other coordinator brokers in the destination cluster.
 */
class InterBrokerSender extends InterBrokerSendThread {
    private final ConcurrentLinkedQueue<RequestAndCompletionHandler> queue = new ConcurrentLinkedQueue<>();

    InterBrokerSender(String name, KafkaClient networkClient, int requestTimeoutMs, Time time) {
        super(name, networkClient, requestTimeoutMs, time);
    }

    public void enqueue(RequestAndCompletionHandler requestAndCompletionHandler) {
        queue.offer(requestAndCompletionHandler);
        wakeup();
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        List<RequestAndCompletionHandler> requests = new ArrayList<>();
        RequestAndCompletionHandler request;
        while ((request = queue.poll()) != null) {
            requests.add(request);
        }
        return requests;
    }
}
