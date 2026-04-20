package com.bounteous.flowtide.controller.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks all active members and their partition assignments
 * for a single consumer group + topic combination.
 *
 * <p>Key: "groupId:topic" — stored in ConsumerGroupService.
 */
@Getter
@Setter
public class ConsumerGroupState {

    private final String groupId;
    private final String topic;

    /** memberId → ConsumerMember */
    private final ConcurrentHashMap<String, ConsumerMember> members = new ConcurrentHashMap<>();

    private volatile long lastRebalanceAt = System.currentTimeMillis();

    public ConsumerGroupState(String groupId, String topic) {
        this.groupId = groupId;
        this.topic   = topic;
    }

    public void addMember(ConsumerMember member) {
        members.put(member.getMemberId(), member);
    }

    public void removeMember(String memberId) {
        members.remove(memberId);
    }

    public ConsumerMember getMember(String memberId) {
        return members.get(memberId);
    }

    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }

    public Set<String> getMemberIds() {
        return Collections.unmodifiableSet(members.keySet());
    }

    public Map<String, ConsumerMember> getAllMembers() {
        return Collections.unmodifiableMap(members);
    }

    public boolean isEmpty() {
        return members.isEmpty();
    }
}
