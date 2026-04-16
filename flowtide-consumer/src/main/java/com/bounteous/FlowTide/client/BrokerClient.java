package com.bounteous.FlowTide.client;

import com.bounteous.FlowTide.model.Event;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

/**
 * Feign client for the FlowTide broker's internal fetch and metadata endpoints.
 *
 * <p>"flowtide-broker" is resolved via Eureka service discovery.
 * Spring Cloud LoadBalancer handles client-side load balancing across
 * all registered broker instances automatically.
 */
@FeignClient(name = "flowtide-broker")
public interface BrokerClient {

    /**
     * Fetches events from a specific topic-partition starting at the given offset.
     */
    @GetMapping("/internal/events/{topic}/{partition}")
    List<Event> fetchEvents(@PathVariable("topic") String topic,
                            @PathVariable("partition") int partition,
                            @RequestParam("offset") long offset,
                            @RequestParam("limit") int limit);

    /**
     * Returns partition metadata for a topic.
     * Response: { "partitionCount": 3 }
     */
    @GetMapping("/internal/metadata/{topic}")
    Map<String, Integer> getMetadata(@PathVariable("topic") String topic);
}
