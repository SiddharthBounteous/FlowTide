package com.bounteous.FlowTide.client;

import com.bounteous.FlowTide.model.JoinRequest;
import com.bounteous.FlowTide.model.JoinResponse;
import com.bounteous.FlowTide.model.TopicMetadata;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Feign client for the consumer group coordinator in flowtide-controller.
 *
 * <p>All consumer group state lives in the controller — not locally in
 * the consumer service — so multiple consumer service instances never
 * produce conflicting partition assignments.
 */

@FeignClient(name = "flowtide-controller")
public interface CoordinatorClient {

    /**
     * Join a consumer group.
     * The coordinator generates a unique memberId and returns assigned partitions.
     */
    @PostMapping("/controller/groups/{groupId}/join")
    JoinResponse join(@PathVariable("groupId") String groupId,
                      @RequestBody JoinRequest request);

    /**
     * Get the partitions currently assigned to a member.
     * Also acts as a heartbeat — keeps the member alive.
     */
    @GetMapping("/controller/groups/{groupId}/assignment")
    List<Integer> getAssignment(@PathVariable("groupId") String groupId,
                                @RequestParam("memberId") String memberId,
                                @RequestParam("topic") String topic);

    /**
     * Send an explicit heartbeat (optional — getAssignment already does this).
     */
    @PostMapping("/controller/groups/{groupId}/members/{memberId}/heartbeat")
    String heartbeat(@PathVariable("groupId") String groupId,
                     @PathVariable("memberId") String memberId,
                     @RequestParam("topic") String topic);

    /**
     * Leave a consumer group.
     * Remaining members are rebalanced immediately.
     */
    @DeleteMapping("/controller/groups/{groupId}/members/{memberId}")
    String leave(@PathVariable("groupId") String groupId,
                 @PathVariable("memberId") String memberId,
                 @RequestParam("topic") String topic,
                 @RequestParam("partitionCount") int partitionCount);

    /**
     * Fetch full topic metadata including partition→leader map.
     * Used by the consumer to build its local leader cache so it can
     * call leader brokers directly without going through load balancing.
     */
    @GetMapping("/controller/topics/{topic}")
    TopicMetadata getTopicMetadata(@PathVariable("topic") String topic);
}
