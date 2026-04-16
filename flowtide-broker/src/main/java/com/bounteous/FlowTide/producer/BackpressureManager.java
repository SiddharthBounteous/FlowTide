package com.bounteous.FlowTide.producer;

import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Detects overload conditions and enforces producer throttling.
 *
 * <p>Tracks the total number of events currently stored across all partitions.
 * When the threshold is exceeded:
 * <ul>
 *   <li>Producers receive HTTP 429 (Too Many Requests) with a Retry-After header.
 *   <li>The system gracefully degrades rather than crashing.
 * </ul>
 *
 * <p>Thresholds are configurable via {@code application.properties}.
 */
@Component
public class BackpressureManager {

    private static final Logger log = LoggerFactory.getLogger(BackpressureManager.class);

    private final LogManager logManager;

    /** Total events allowed across all partitions before throttling kicks in. */
    @Value("${kafka.backpressure.max-total-events:5000000}")
    private long maxTotalEvents;

    /** Estimated bytes allowed in memory before throttling kicks in (default: 1 GB). */
    @Value("${kafka.backpressure.max-total-bytes:1073741824}")
    private long maxTotalBytes;

    /** How many seconds the client should wait before retrying (sent as Retry-After header). */
    @Value("${kafka.backpressure.retry-after-seconds:5}")
    private int retryAfterSeconds;

    private final AtomicLong throttledRequestCount = new AtomicLong(0);

    public BackpressureManager(LogManager logManager) {
        this.logManager = logManager;
    }

    /**
     * Returns {@code true} if the system is currently overloaded and producers
     * should be throttled.
     */
    public boolean isOverloaded() {
        long totalEvents = logManager.totalEventsStored();
        long totalBytes  = logManager.totalBytesEstimated();

        if (totalEvents > maxTotalEvents || totalBytes > maxTotalBytes) {
            throttledRequestCount.incrementAndGet();
            log.warn("Backpressure active: events={}/{} bytes={}/{} MB",
                    totalEvents, maxTotalEvents,
                    totalBytes / (1024 * 1024), maxTotalBytes / (1024 * 1024));
            return true;
        }
        return false;
    }

    public int getRetryAfterSeconds() {
        return retryAfterSeconds;
    }

    public long getThrottledRequestCount() {
        return throttledRequestCount.get();
    }

    public long getTotalEventsStored() {
        return logManager.totalEventsStored();
    }

    public long getTotalBytesEstimated() {
        return logManager.totalBytesEstimated();
    }
}
