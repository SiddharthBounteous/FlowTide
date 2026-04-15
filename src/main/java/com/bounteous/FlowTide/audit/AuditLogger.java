package com.bounteous.FlowTide.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Structured audit logger for all administrative and cluster events.
 *
 * <p>Writes to a dedicated "AUDIT" logger so audit entries can be routed to a
 * separate log file or SIEM system via logback configuration, independent of
 * the application log.
 *
 * <p>Logged events include: topic create/delete, node join/leave, rebalance,
 * failover, consumer group changes.
 */
@Component
public class AuditLogger {

    private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

    /**
     * Logs an audit event.
     *
     * @param action  e.g. "TOPIC_CREATED", "NODE_JOINED", "LEADER_FAILOVER"
     * @param details key=value pairs describing the event
     */
    public void log(String action, String details) {
        AUDIT.info("[{}] action={} {}", Instant.now(), action, details);
    }

    /** Convenience overload for simple single-key events. */
    public void log(String action, String key, String value) {
        log(action, key + "=" + value);
    }
}
