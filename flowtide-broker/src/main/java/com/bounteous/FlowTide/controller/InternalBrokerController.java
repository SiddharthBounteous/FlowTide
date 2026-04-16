package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.producer.ProducerService;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Internal HTTP API used exclusively by flowtide-producer and flowtide-consumer.
 *
 * <p>The gateway does NOT expose /internal/** to the outside world.
 *
 * <ul>
 *   <li>POST /internal/events              — publish an event (backpressure + metrics applied)
 *   <li>GET  /internal/events/{topic}/{p}  — fetch events from a partition
 *   <li>GET  /internal/metadata/{topic}    — partition count for a topic
 * </ul>
 */
@RestController
@RequestMapping("/internal")
public class InternalBrokerController {

    private static final Logger log = LoggerFactory.getLogger(InternalBrokerController.class);

    private final ProducerService producerService;
    private final LogManager logManager;

    public InternalBrokerController(ProducerService producerService, LogManager logManager) {
        this.producerService = producerService;
        this.logManager = logManager;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Produce
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/events")
    public ResponseEntity<PublishResponse> storeEvent(@RequestBody PublishRequest request) {
        return ResponseEntity.ok(producerService.publish(request));
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Fetch
    // ─────────────────────────────────────────────────────────────────────────

    @GetMapping("/events/{topic}/{partition}")
    public List<Event> fetchEvents(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam(defaultValue = "0") long offset,
            @RequestParam(defaultValue = "100") int limit) {

        List<Event> events = logManager.getLog(topic, partition).readFrom(offset, limit);
        log.debug("Fetched {} events: topic={} partition={} offset={}", events.size(), topic, partition, offset);
        return events;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Metadata
    // ─────────────────────────────────────────────────────────────────────────

    @GetMapping("/metadata/{topic}")
    public Map<String, Integer> getMetadata(@PathVariable String topic) {
        int count = logManager.getTopicLogs(topic).size();
        if (count == 0) {
            // Auto-create default partitions on first metadata request
            int defaultPartitions = 3;
            for (int i = 0; i < defaultPartitions; i++) logManager.getLog(topic, i);
            count = defaultPartitions;
        }
        return Map.of("partitionCount", count);
    }
}
