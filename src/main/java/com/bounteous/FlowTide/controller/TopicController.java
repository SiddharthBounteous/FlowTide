package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.topic.CreateTopicRequest;
import com.bounteous.FlowTide.topic.TopicConfig;
import com.bounteous.FlowTide.topic.TopicManager;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

/**
 * REST API for topic lifecycle management.
 *
 * <p>Base path: {@code /api/topics}
 */
@RestController
@RequestMapping("/api/topics")
public class TopicController {

    private final TopicManager topicManager;

    public TopicController(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    /** Create a new topic with configurable partitions, replication, and retention. */
    @PostMapping
    public ResponseEntity<TopicConfig> createTopic(@RequestBody @Valid CreateTopicRequest request) {
        TopicConfig config = topicManager.createTopic(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(config);
    }

    /** List all known topics. */
    @GetMapping
    public Collection<String> listTopics() {
        return topicManager.listTopics();
    }

    /** Get full configuration for a specific topic. */
    @GetMapping("/{topic}")
    public ResponseEntity<TopicConfig> getTopic(@PathVariable String topic) {
        return topicManager.getTopic(topic)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /** Delete a topic and all its stored events. */
    @DeleteMapping("/{topic}")
    public ResponseEntity<String> deleteTopic(@PathVariable String topic) {
        topicManager.deleteTopic(topic);
        return ResponseEntity.ok("Topic '" + topic + "' deleted");
    }
}
