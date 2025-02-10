// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package kafka.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * The response queue for a connection.
 *
 * <p>This queue arranges responses by their correlation ID, expecting no gaps and the strict order.
 */
class InklessSendQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(InklessSendQueue.class);

    private static final Comparator<RequestChannel.SendResponse> CORRELATION_ID_COMPARATOR =
        Comparator.comparing((RequestChannel.SendResponse r) -> r.request().header().correlationId());
    private final PriorityQueue<RequestChannel.SendResponse> queue = new PriorityQueue<>(CORRELATION_ID_COMPARATOR);
    private int nextCorrelationId;

    InklessSendQueue(final int startCorrelationId) {
//        LOGGER.info("Starting with correlation ID {}", startCorrelationId);
        this.nextCorrelationId = startCorrelationId;
    }

    void add(final RequestChannel.SendResponse response) {
//        LOGGER.info("Adding response with correlation ID {}", response.request().header().correlationId());
        if (response.request().header().correlationId() < nextCorrelationId) {
            throw new IllegalStateException("Expected min correlation ID " + nextCorrelationId);
        }
        queue.add(response);
    }

    boolean nextReady() {
        final RequestChannel.SendResponse peeked = queue.peek();
        if (peeked == null) {
            return false;
        }
        final int correlationId = peeked.request().header().correlationId();
//        LOGGER.info("Peeked correlation ID {}, expecting {}", peeked.request().header().correlationId(), nextCorrelationId);
        return correlationId == nextCorrelationId;
    }

    RequestChannel.SendResponse take() {
        if (!nextReady()) {
            throw new IllegalStateException();
        }
        nextCorrelationId += 1;
        return queue.remove();
    }
}
