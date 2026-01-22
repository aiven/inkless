package kafka.server.mirror;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class InterBrokerSender extends InterBrokerSendThread {
    ConcurrentLinkedQueue<RequestAndCompletionHandler> queue = new ConcurrentLinkedQueue<>();
    protected InterBrokerSender(String name, KafkaClient networkClient, int requestTimeoutMs, Time time) {
        super(name, networkClient, requestTimeoutMs, time);
    }

    public void enqueue(RequestAndCompletionHandler requestAndCompletionHandler) {
        queue.offer(requestAndCompletionHandler);
        wakeup();
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        if (!queue.isEmpty()) {
            List<RequestAndCompletionHandler> requests = new ArrayList<>();
            while (queue.peek() != null) {
                var requestAndCompletionHandler = queue.poll();
                requests.add(requestAndCompletionHandler);
            }
            return requests;
        }
        return List.of();
    }
}
