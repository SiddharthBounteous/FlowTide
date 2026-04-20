package com.bounteous.FlowTide.producer;

import com.bounteous.FlowTide.metrics.MetricsService;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import com.bounteous.FlowTide.replication.ISRException;
import com.bounteous.FlowTide.replication.ISRManager;
import com.bounteous.FlowTide.server.log.LogManager;
import com.bounteous.FlowTide.topic.TopicManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * Publishes events to the local log and replicates synchronously to ISR followers.
 *
 * <h3>Write flow</h3>
 * <pre>
 * 1. Backpressure check — reject if broker is overloaded
 * 2. Append to local PartitionLog (in-memory, immediate)
 * 3. ISRManager.replicateToISR() — push to all in-sync followers, wait for ACK
 *    - If all ISR followers ACK → return success to producer ✓
 *    - If a follower doesn't ACK → remove from ISR, continue with remaining ISR
 *    - If ISR size < min-isr after removals → throw ISRException (HTTP 503)
 * 4. Return PublishResponse with offset
 * </pre>
 *
 * <h3>Durability guarantee</h3>
 * With min-isr=2, every acknowledged write is in memory on at least 2 brokers.
 * A single broker crash cannot cause data loss.
 */
@Service
public class ProducerService {

    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private final LogManager          logManager;
    private final BackpressureManager backpressure;
    private final MetricsService      metrics;
    private final ISRManager          isrManager;
    private final TopicManager        topicManager;

    @Value("${kafka.topic.default-partitions:3}")
    private int defaultPartitions;

    @Value("${kafka.topic.default-replication-factor:1}")
    private int defaultReplicationFactor;

    public ProducerService(LogManager logManager,
                           BackpressureManager backpressure,
                           MetricsService metrics,
                           ISRManager isrManager,
                           TopicManager topicManager) {
        this.logManager         = logManager;
        this.backpressure       = backpressure;
        this.metrics            = metrics;
        this.isrManager         = isrManager;
        this.topicManager       = topicManager;
    }

    public PublishResponse publish(PublishRequest request) {
        checkBackpressure();

        // Auto-create the topic if this broker has never seen it.
        // This ensures MetadataController and flowtide-controller both learn
        // about the topic — not just the in-memory PartitionLog.
        if (!logManager.topicExists(request.getTopic())) {
            log.info("Auto-creating topic '{}' on first publish", request.getTopic());
            topicManager.autoCreate(request.getTopic(), defaultPartitions, defaultReplicationFactor);
        }

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

        // Step 1 — append to local log (fast, in-memory)
        long offset = logManager.getLog(request.getTopic(), partition).append(event);

        // Step 2 — synchronously replicate to ISR followers before acknowledging
        try {
            isrManager.replicateToISR(request.getTopic(), partition, event);
        } catch (ISRException e) {
            // ISR too small — roll back local append is complex in a skip-list,
            // so we surface a 503 and let the producer retry.
            // The event is in memory but will be lost if this broker restarts
            // before ISR recovers — acceptable trade-off for in-memory mode.
            log.error("ISR violation on publish: {}", e.getMessage());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        }

        metrics.recordPublish(event.getTopic(), partition);

        log.info("Published+ISR: topic={} partition={} offset={} isrSize={}",
                event.getTopic(), partition, offset,
                isrManager.getISRSize(request.getTopic(), partition));

        return new PublishResponse(
                event.getId(), event.getTopic(), partition, offset, event.getTimestamp());
    }

    public List<PublishResponse> publishBatch(List<PublishRequest> requests) {
        checkBackpressure();
        return requests.stream().map(this::publish).toList();
    }

    // -------------------------------------------------------------------------

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
