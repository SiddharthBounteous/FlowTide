package com.bounteous.FlowTide.producer;

import com.bounteous.FlowTide.metrics.MetricsService;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * Publishes events to the local LogManager over REST (no TCP).
 *
 * <p>Flow:
 * <pre>
 * InternalBrokerController
 *   → ProducerService.publish()     (backpressure check + partition selection)
 *   → LogManager.getLog().append()  (in-memory PartitionLog, same JVM)
 *   → PublishResponse               (eventId, topic, partition, offset, timestamp)
 * </pre>
 */
@Service
public class ProducerService {

    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private final LogManager logManager;
    private final BackpressureManager backpressure;
    private final MetricsService metrics;

    @Value("${kafka.topic.default-partitions:3}")
    private int defaultPartitions;

    public ProducerService(LogManager logManager,
                           BackpressureManager backpressure,
                           MetricsService metrics) {
        this.logManager = logManager;
        this.backpressure = backpressure;
        this.metrics = metrics;
    }

    public PublishResponse publish(PublishRequest request) {
        checkBackpressure();

        int partitionCount = resolvePartitionCount(request.getTopic());
        int partition      = selectPartition(request.getKey(), partitionCount);

        Event event = Event.builder()
                .topic(request.getTopic())
                .key(request.getKey())
                .payload(request.getPayload())
                .headers(request.getHeaders())
                .deliveryMode(request.getDeliveryMode())
                .partition(partition)
                .build();

        long offset = logManager.getLog(request.getTopic(), partition).append(event);
        metrics.recordPublish(event.getTopic(), partition);

        log.info("Published: topic={} partition={} offset={} key={}",
                event.getTopic(), partition, offset, event.getKey());

        return new PublishResponse(
                event.getId(), event.getTopic(), partition, offset, event.getTimestamp());
    }

    public List<PublishResponse> publishBatch(List<PublishRequest> requests) {
        checkBackpressure();
        return requests.stream().map(this::publish).toList();
    }

    // ─────────────────────────────────────────────────────────────────────────

    private void checkBackpressure() {
        if (backpressure.isOverloaded()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,
                    "Broker is overloaded. Retry after " + backpressure.getRetryAfterSeconds() + "s.");
        }
    }

    private int resolvePartitionCount(String topic) {
        int count = logManager.getTopicLogs(topic).size();
        return count == 0 ? defaultPartitions : count;
    }

    private int selectPartition(String key, int numPartitions) {
        if (key == null || key.isBlank()) {
            return (int) (Thread.currentThread().getId() % numPartitions);
        }
        return Math.abs(key.hashCode()) % numPartitions;
    }
}
