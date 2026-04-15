package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.model.BatchPublishRequest;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import com.bounteous.FlowTide.producer.ProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * REST API for publishing events.
 *
 * <p>Base path: {@code /api/producer}
 *
 * <p>Supports single-event publish and batch publish.
 * Delivery guarantee (AT_MOST_ONCE / AT_LEAST_ONCE) is set per-event in the request body.
 */
@RestController
@RequestMapping("/api/producer")
public class ProducerController {

    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * Publish a single event.
     *
     * <p>Returns 201 Created with the assigned topic, partition, and offset.
     * Returns 429 Too Many Requests if the broker is overloaded (with Retry-After header).
     */
    @PostMapping("/send")
    public ResponseEntity<PublishResponse> sendMessage(@RequestBody @Valid PublishRequest request) {
        try {
            PublishResponse response = producerService.publish(request);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (ResponseStatusException e) {
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.RETRY_AFTER, extractRetryAfter(e.getReason()));
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).headers(headers).build();
        }
    }

    /**
     * Publish a batch of events.
     *
     * <p>Each event is routed independently. Returns a response for each event.
     */
    @PostMapping("/send/batch")
    public ResponseEntity<List<PublishResponse>> sendBatch(@RequestBody @Valid BatchPublishRequest batch) {
        try {
            List<PublishResponse> responses = producerService.publishBatch(batch.getEvents());
            return ResponseEntity.status(HttpStatus.CREATED).body(responses);
        } catch (ResponseStatusException e) {
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.RETRY_AFTER, extractRetryAfter(e.getReason()));
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).headers(headers).build();
        }
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Producer API is running");
    }

    private String extractRetryAfter(String reason) {
        if (reason != null && reason.contains("Retry after")) {
            try {
                return reason.replaceAll("\\D+", "").trim();
            } catch (Exception ignored) {}
        }
        return "5";
    }
}
