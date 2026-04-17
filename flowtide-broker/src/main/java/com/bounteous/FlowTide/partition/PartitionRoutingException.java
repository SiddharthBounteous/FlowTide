package com.bounteous.FlowTide.partition;

/**
 * Thrown when a produce or fetch request cannot be routed to the correct
 * leader broker (e.g. the controller returns no leader, or the network call
 * to the leader fails).
 */
public class PartitionRoutingException extends RuntimeException {

    public PartitionRoutingException(String message) {
        super(message);
    }

    public PartitionRoutingException(String message, Throwable cause) {
        super(message, cause);
    }
}
