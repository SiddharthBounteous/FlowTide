package com.bounteous.flowtide.controller.api;

import com.bounteous.flowtide.controller.model.ConsumerGroupState;
import com.bounteous.flowtide.controller.model.JoinRequest;
import com.bounteous.flowtide.controller.model.JoinResponse;
import com.bounteous.flowtide.controller.service.ConsumerGroupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Consumer group coordinator API — served by flowtide-controller.
 *
 * <p>This is the single source of truth for group membership across all
 * flowtide-consumer instances.  Every consumer service instance calls
 * these endpoints instead of managing group state locally.
 *
 * <ul>
 *   <li>POST   /controller/groups/{groupId}/join                     — join group, get memberId
 *   <li>POST   /controller/groups/{groupId}/members/{memberId}/heartbeat — keep-alive
 *   <li>GET    /controller/groups/{groupId}/assignment               — get assigned partitions
 *   <li>DELETE /controller/groups/{groupId}/members/{memberId}       — leave group
 *   <li>GET    /controller/groups                                    — list all groups (admin)
 *   <li>GET    /controller/groups/{groupId}                          — group detail (admin)
 * </ul>
 */
@RestController
@RequestMapping("/controller/groups")
public class ConsumerCoordinatorController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinatorController.class);

    private final ConsumerGroupService groupService;

    public ConsumerCoordinatorController(ConsumerGroupService groupService) {
        this.groupService = groupService;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  JOIN
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * A consumer joins a group.
     *
     * <p>The coordinator:
     * <ol>
     *   <li>Generates a unique memberId.
     *   <li>Adds the member to the group.
     *   <li>Rebalances partitions across all active members.
     *   <li>Returns the memberId and assigned partitions.
     * </ol>
     *
     * @param groupId group name
     * @param request topic + partitionCount
     * @return JoinResponse with coordinator-assigned memberId and partition list
     */
    @PostMapping("/{groupId}/join")
    public ResponseEntity<JoinResponse> join(
            @PathVariable String groupId,
            @RequestBody JoinRequest request) {
        JoinResponse response = groupService.join(groupId, request.getTopic(), request.getPartitionCount());
        return ResponseEntity.ok(response);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  HEARTBEAT
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Refreshes a member's heartbeat so the coordinator doesn't evict it.
     * Called by consumer service on each poll cycle.
     */
    @PostMapping("/{groupId}/members/{memberId}/heartbeat")
    public ResponseEntity<String> heartbeat(
            @PathVariable String groupId,
            @PathVariable String memberId,
            @RequestParam String topic) {
        groupService.heartbeat(groupId, memberId, topic);
        return ResponseEntity.ok("OK");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  GET ASSIGNMENT
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the partition list assigned to a member.
     * Also counts as a heartbeat — no separate HB needed on every poll.
     */
    @GetMapping("/{groupId}/assignment")
    public ResponseEntity<List<Integer>> getAssignment(
            @PathVariable String groupId,
            @RequestParam String memberId,
            @RequestParam String topic) {
        List<Integer> partitions = groupService.getAssignment(groupId, memberId, topic);
        return ResponseEntity.ok(partitions);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  LEAVE
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Removes a member from a group and rebalances remaining members.
     */
    @DeleteMapping("/{groupId}/members/{memberId}")
    public ResponseEntity<String> leave(
            @PathVariable String groupId,
            @PathVariable String memberId,
            @RequestParam String topic,
            @RequestParam(defaultValue = "3") int partitionCount) {
        groupService.leave(groupId, memberId, topic, partitionCount);
        return ResponseEntity.ok("Member " + memberId + " left group " + groupId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  EXCEPTION HANDLING
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns 404 when a memberId is not found (e.g. controller restarted and
     * lost in-memory state, or the member was evicted due to missed heartbeats).
     *
     * <p>The consumer catches this 404 and automatically rejoins the group,
     * so the end-user never sees an error.
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleUnknownMember(IllegalArgumentException ex) {
        log.warn("Unknown member or group: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  ADMIN
    // ─────────────────────────────────────────────────────────────────────────

    /** Returns all consumer groups and their current membership. */
    @GetMapping
    public Map<String, ConsumerGroupState> getAllGroups() {
        return groupService.getAllGroups();
    }

    /** Returns detail for a specific group+topic. */
    @GetMapping("/{groupId}")
    public ResponseEntity<?> getGroup(
            @PathVariable String groupId,
            @RequestParam String topic) {
        return groupService.getGroup(groupId, topic)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
