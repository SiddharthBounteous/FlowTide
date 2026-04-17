package com.bounteous.FlowTide.client;

import com.bounteous.FlowTide.client.model.BrokerRegistration;
import com.bounteous.FlowTide.client.model.HeartbeatRequest;
import com.bounteous.FlowTide.client.model.PartitionAssignment;
import com.bounteous.FlowTide.client.model.TopicMetadata;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

/**
 * Feign client for the flowtide-controller service.
 *
 * <p>Used by each broker to:
 * <ul>
 *   <li>Register itself on startup
 *   <li>Send periodic heartbeats
 *   <li>Look up partition leader for routing decisions
 * </ul>
 */
@FeignClient(name = "flowtide-controller")
public interface ControllerClient {

    /** Register this broker with the controller on startup. */
    @PostMapping("/controller/brokers/register")
    PartitionAssignment register(@RequestBody BrokerRegistration registration);

    /** Send heartbeat to the controller. */
    @PostMapping("/controller/brokers/heartbeat")
    String heartbeat(@RequestBody HeartbeatRequest request);

    /** Get full topic metadata including leader map. */
    @GetMapping("/controller/topics/{topic}")
    TopicMetadata getTopicMetadata(@PathVariable("topic") String topic);

    /** Get the leader broker id for a specific partition. */
    @GetMapping("/controller/topics/{topic}/{partition}/leader")
    String getLeader(@PathVariable("topic") String topic,
                     @PathVariable("partition") int partition);
}
