package com.bounteous.FlowTide.producer;

import com.bounteous.FlowTide.clients.producer.MyProducer;
import com.bounteous.FlowTide.clients.producer.RecordMetaData;
import com.bounteous.FlowTide.metrics.MetricsService;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;

/**
 * Orchestrates event publishing.
 *
 * <p><strong>The key fix:</strong> this service no longer writes directly to
 * {@code LogManager}. Every event travels the full TCP path:
 *
 * <pre>
 * REST API
 *   → ProducerService          (validates, checks backpressure)
 *   → MyProducer.sendEvent()   (asks Registry: "who is leader for this partition?")
 *   → TCP socket               (connects to the correct broker JVM)
 *   → RequestHandler           (receives Event, appends to PartitionLog, replies with offset)
 *   → back: real offset
 * </pre>
 *
 * <p>This means in a multi-broker cluster, the event reaches the <em>correct</em>
 * broker — not just whichever JVM the REST API happens to run on.
 */
@Service
public class ProducerService {

    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private final MyProducer<String, String> myProducer;
    private final BackpressureManager backpressure;
    private final MetricsService metrics;

    public ProducerService(MyProducer<String, String> myProducer,
                           BackpressureManager backpressure,
                           MetricsService metrics) {
        this.myProducer = myProducer;
        this.backpressure = backpressure;
        this.metrics = metrics;
    }

    /**
     * Publishes a single event through the TCP broker path.
     *
     * @throws ResponseStatusException HTTP 429 if the broker is overloaded
     * @throws RuntimeException if the TCP send or broker storage fails
     */
    public PublishResponse publish(PublishRequest request) {
        checkBackpressure();

        // Build the rich Event — partition will be assigned by MyProducer
        // based on live registry metadata (key-hash or round-robin)
        Event event = Event.builder()
                .topic(request.getTopic())
                .key(request.getKey())
                .payload(request.getPayload())
                .headers(request.getHeaders())
                .deliveryMode(request.getDeliveryMode())
                .build();

        try {
            // ↓ This goes: MyProducer → Registry → TCP socket → Broker → LogManager
            RecordMetaData meta = myProducer.sendEvent(event).get();

            metrics.recordPublish(meta.getTopic(), meta.getPartition());
            log.info("Published: topic={} partition={} offset={} key={}",
                    meta.getTopic(), meta.getPartition(), meta.getOffset(), event.getKey());

            return new PublishResponse(
                    event.getId(), meta.getTopic(), meta.getPartition(),
                    meta.getOffset(), event.getTimestamp());

        } catch (Exception e) {
            throw new RuntimeException("Failed to publish event to topic: " + request.getTopic(), e);
        }
    }

    /**
     * Publishes a batch of events. Each event is routed independently
     * so events with the same key land on the same partition.
     */
    public List<PublishResponse> publishBatch(List<PublishRequest> requests) {
        checkBackpressure();

        List<PublishResponse> responses = new ArrayList<>(requests.size());
        for (PublishRequest req : requests) {
            responses.add(publish(req));
        }
        return responses;
    }

    private void checkBackpressure() {
        if (backpressure.isOverloaded()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,
                    "Broker is overloaded. Retry after " + backpressure.getRetryAfterSeconds() + " seconds.");
        }
    }
}
