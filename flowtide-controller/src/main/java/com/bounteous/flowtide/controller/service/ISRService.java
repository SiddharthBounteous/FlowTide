package com.bounteous.flowtide.controller.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central ISR (In-Sync Replica) registry.
 *
 * <p>The leader broker reports ISR changes here after every ACK success/failure.
 * Consumers and admin tooling can query this to know which replicas are safe
 * to read from.
 *
 * <p>Structure: topic → partition → Set of in-sync broker IDs (including leader).
 */
@Service
public class ISRService {

    private static final Logger log = LoggerFactory.getLogger(ISRService.class);

    /** topic → partition → Set<brokerId> (all in-sync replicas including leader) */
    private final ConcurrentHashMap<String, Map<Integer, Set<String>>> isrMap
            = new ConcurrentHashMap<>();

    /**
     * Called by a leader broker whenever its ISR changes.
     *
     * @param topic      topic name
     * @param partition  partition index
     * @param leaderId   the leader broker ID (always included in ISR)
     * @param followers  current in-sync follower IDs (may be empty)
     */
    public void updateISR(String topic, int partition, String leaderId, List<String> followers) {
        Set<String> isr = new LinkedHashSet<>();
        isr.add(leaderId);
        isr.addAll(followers);

        isrMap.computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
              .put(partition, isr);

        log.info("ISR updated: topic={} partition={} isr={}", topic, partition, isr);
    }

    /** Returns the current ISR for a partition. Empty set if unknown. */
    public Set<String> getISR(String topic, int partition) {
        Map<Integer, Set<String>> topicISR = isrMap.get(topic);
        if (topicISR == null) return Collections.emptySet();
        return Collections.unmodifiableSet(topicISR.getOrDefault(partition, Collections.emptySet()));
    }

    /** Returns ISR size for a partition (0 if unknown). */
    public int getISRSize(String topic, int partition) {
        return getISR(topic, partition).size();
    }

    /** Returns all ISR data (for admin API). */
    public Map<String, Map<Integer, Set<String>>> getAllISR() {
        return Collections.unmodifiableMap(isrMap);
    }
}
