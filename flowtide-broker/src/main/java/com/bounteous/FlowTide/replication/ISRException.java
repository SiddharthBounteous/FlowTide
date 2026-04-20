package com.bounteous.FlowTide.replication;

/**
 * Thrown when a write cannot be safely acknowledged because the number of
 * in-sync replicas has dropped below {@code kafka.replication.min-isr}.
 *
 * <p>Maps to HTTP 503 Service Unavailable — the producer should retry
 * once ISR recovers.
 */
public class ISRException extends RuntimeException {
    public ISRException(String message) {
        super(message);
    }
}
