package com.bounteous.FlowTide.consumer;

import lombok.Data;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Tracks the members and partition assignments of a single consumer group. */
@Data
public class ConsumerGroupInfo {

    private final String groupId;
    private final String topic;

    /** memberId → list of assigned partition indices */
    private final Map<String, List<Integer>> assignments = new ConcurrentHashMap<>();

    /** memberId → last heartbeat timestamp */
    private final Map<String, Long> memberHeartbeats = new ConcurrentHashMap<>();

    private volatile long lastRebalanceAt = System.currentTimeMillis();

    public void addMember(String memberId) {
        assignments.putIfAbsent(memberId, new ArrayList<>());
        memberHeartbeats.put(memberId, System.currentTimeMillis());
    }

    public void removeMember(String memberId) {
        assignments.remove(memberId);
        memberHeartbeats.remove(memberId);
    }

    public Set<String> getMembers() {
        return Collections.unmodifiableSet(assignments.keySet());
    }

    public List<Integer> getAssignedPartitions(String memberId) {
        return assignments.getOrDefault(memberId, Collections.emptyList());
    }

    public void updateHeartbeat(String memberId) {
        memberHeartbeats.put(memberId, System.currentTimeMillis());
    }

    public boolean hasMember(String memberId) {
        return assignments.containsKey(memberId);
    }
}
