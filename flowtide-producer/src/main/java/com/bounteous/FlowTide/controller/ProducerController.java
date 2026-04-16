package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.model.BatchPublishRequest;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import com.bounteous.FlowTide.producer.ProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/producer")
public class ProducerController {

    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/send")
    public ResponseEntity<PublishResponse> send(@RequestBody @Valid PublishRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(producerService.publish(request));
    }

    @PostMapping("/send/batch")
    public ResponseEntity<List<PublishResponse>> sendBatch(@RequestBody @Valid BatchPublishRequest batch) {
        return ResponseEntity.status(HttpStatus.CREATED).body(producerService.publishBatch(batch.getEvents()));
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Producer service is running");
    }
}
