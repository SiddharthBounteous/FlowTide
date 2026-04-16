package com.bounteous.FlowTide.clients.http;

/**
 * Thrown by {@link FlowTideHttpProducer} and {@link FlowTideHttpConsumer}
 * when an HTTP call fails or returns an unexpected status code.
 */
public class FlowTideHttpException extends RuntimeException {

    private final int statusCode;

    public FlowTideHttpException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public FlowTideHttpException(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = -1;
    }

    /** HTTP status code returned by the server, or -1 if the request never reached the server. */
    public int getStatusCode() {
        return statusCode;
    }

    public boolean isBackpressure() {
        return statusCode == 429;
    }
}
