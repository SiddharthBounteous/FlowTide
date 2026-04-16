package com.bounteous.FlowTide.client;

import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * Feign client for the FlowTide broker's internal produce endpoint.
 *
 * <p>"flowtide-broker" is resolved via Eureka service discovery.
 * Spring Cloud LoadBalancer handles client-side load balancing across
 * all registered broker instances automatically.
 */
@FeignClient(name = "flowtide-broker")
public interface BrokerClient {

    /**
     * Publishes a single event to the broker.
     * The broker selects the partition, appends to PartitionLog, and returns the offset.
     */
    @PostMapping("/internal/events")
    PublishResponse send(@RequestBody PublishRequest request);
}
