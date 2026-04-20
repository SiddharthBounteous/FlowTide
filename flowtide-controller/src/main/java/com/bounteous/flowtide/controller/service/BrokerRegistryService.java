package com.bounteous.flowtide.controller.service;

import com.bounteous.flowtide.controller.model.BrokerInfo;
import com.bounteous.flowtide.controller.model.BrokerRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks all registered brokers and their heartbeat state.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Register new brokers on startup
 *   <li>Update heartbeat timestamps on every heartbeat call
 *   <li>Mark brokers as dead when heartbeat times out
 *   <li>Return active broker list for partition assignment
 * </ul>
 */
@Service
public class BrokerRegistryService {

    private static final Logger log = LoggerFactory.getLogger(BrokerRegistryService.class);

    /** brokerId ("host:port") → BrokerInfo */
    private final ConcurrentHashMap<String, BrokerInfo> brokers = new ConcurrentHashMap<>();

    // ─────────────────────────────────────────────────────────────────────────
    //  Registration
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Registers a broker or refreshes its heartbeat if already known.
     * Returns true if this is a new broker (triggers partition reassignment).
     */
    public boolean register(BrokerRegistration registration) {
        String id = registration.getId();
        boolean isNew = !brokers.containsKey(id);

        brokers.compute(id, (key, existing) -> {
            if (existing == null) {
                log.info("Broker registered: {}", id);
                return new BrokerInfo(registration.getHost(), registration.getPort());
            }
            existing.updateHeartbeat();
            existing.setActive(true);
            log.info("Broker re-registered (reconnect): {}", id);
            return existing;
        });

        return isNew;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Heartbeat
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Refreshes the heartbeat for a known broker.
     * @return {@code true} if the broker was found, {@code false} if unknown.
     *         The caller (controller) is responsible for auto-registering unknown brokers.
     */
    public boolean heartbeat(String host, int port) {
        String id = host + ":" + port;
        BrokerInfo broker = brokers.get(id);
        if (broker != null) {
            broker.updateHeartbeat();
            broker.setActive(true);
            log.trace("Heartbeat received: {}", id);
            return true;
        }
        return false;
    }

    /** Returns true when the broker is already in the registry (active or dead). */
    public boolean isKnown(String brokerId) {
        return brokers.containsKey(brokerId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Health check
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scans all brokers and marks as dead those whose heartbeat
     * is older than timeoutMs. Returns list of newly dead brokers.
     */
    public List<BrokerInfo> detectDeadBrokers(long timeoutMs) {
        long now = System.currentTimeMillis();
        List<BrokerInfo> dead = new ArrayList<>();

        for (BrokerInfo broker : brokers.values()) {
            if (broker.isActive() && now - broker.getLastHeartbeat() > timeoutMs) {
                broker.markDead();
                dead.add(broker);
                log.warn("Broker declared DEAD (no heartbeat for {}ms): {}", timeoutMs, broker.getId());
            }
        }

        return dead;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Query
    // ─────────────────────────────────────────────────────────────────────────

    public List<BrokerInfo> getActiveBrokers() {
        List<BrokerInfo> active = new ArrayList<>();
        for (BrokerInfo b : brokers.values()) {
            if (b.isActive()) active.add(b);
        }
        // Sort by id for deterministic partition assignment
        active.sort((a, b) -> a.getId().compareTo(b.getId()));
        return Collections.unmodifiableList(active);
    }

    public List<BrokerInfo> getAllBrokers() {
        return Collections.unmodifiableList(new ArrayList<>(brokers.values()));
    }

    public Optional<BrokerInfo> getBroker(String id) {
        return Optional.ofNullable(brokers.get(id));
    }

    public int getActiveBrokerCount() {
        return (int) brokers.values().stream().filter(BrokerInfo::isActive).count();
    }
}
