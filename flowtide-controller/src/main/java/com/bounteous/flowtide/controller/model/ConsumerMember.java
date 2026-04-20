package com.bounteous.flowtide.controller.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

/**
 * A single consumer instance inside a consumer group.
 *
 * <p>Created by the coordinator when a consumer calls JOIN.
 * The memberId is coordinator-generated — clients never choose it.
 */
@Getter
@Setter
@ToString
public class ConsumerMember {

    private final String memberId;
    private final String groupId;
    private final String topic;

    /** Partition indices assigned to this member by the coordinator. */
    private volatile List<Integer> assignedPartitions = Collections.emptyList();

    /** Last time this member sent a heartbeat or polled. */
    private volatile long lastHeartbeat;

    private volatile boolean active;

    public ConsumerMember(String memberId, String groupId, String topic) {
        this.memberId      = memberId;
        this.groupId       = groupId;
        this.topic         = topic;
        this.lastHeartbeat = System.currentTimeMillis();
        this.active        = true;
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        this.active        = true;
    }

    public void markDead() {
        this.active = false;
    }
}
