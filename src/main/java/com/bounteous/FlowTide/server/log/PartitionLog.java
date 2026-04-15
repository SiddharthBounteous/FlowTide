package com.bounteous.FlowTide.server.log;

import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.topic.RetentionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory, ordered log for a single topic-partition.
 *
 * <p>Uses a {@link ConcurrentSkipListMap} keyed by offset so reads and writes
 * are thread-safe without a global lock.  The only synchronized section is
 * inside {@code append} to make offset assignment + insertion atomic.
 *
 * <p>Eviction is driven externally by {@link com.bounteous.FlowTide.server.retention.RetentionScheduler}.
 */
public class PartitionLog {

    private static final Logger log = LoggerFactory.getLogger(PartitionLog.class);

    private final String topic;
    private final int partition;
    private final RetentionPolicy retention;

    /** offset → Event (ordered, skip-list for O(log n) range scans). */
    private final ConcurrentSkipListMap<Long, Event> events = new ConcurrentSkipListMap<>();
    private final AtomicLong nextOffset = new AtomicLong(0);

    /** Running total of estimated bytes stored (payload.length * events). */
    private final AtomicLong estimatedBytes = new AtomicLong(0);

    public PartitionLog(String topic, int partition, RetentionPolicy retention) {
        this.topic = topic;
        this.partition = partition;
        this.retention = retention;
    }

    /**
     * Appends an event and returns the assigned offset.
     * Thread-safe: offset assignment and map insertion are synchronized.
     */
    public synchronized long append(Event event) {
        long offset = nextOffset.getAndIncrement();
        event.setOffset(offset);
        events.put(offset, event);
        estimatedBytes.addAndGet(estimateSize(event));
        return offset;
    }

    /**
     * Reads up to {@code maxMessages} events starting from {@code startOffset} (inclusive).
     */
    public List<Event> readFrom(long startOffset, int maxMessages) {
        if (startOffset < 0 || maxMessages <= 0) {
            return Collections.emptyList();
        }
        List<Event> result = new ArrayList<>(Math.min(maxMessages, 256));
        for (Event e : events.tailMap(startOffset).values()) {
            if (result.size() >= maxMessages) break;
            result.add(e);
        }
        return result;
    }

    /**
     * Returns the earliest (lowest) offset currently stored.
     * Returns {@code nextOffset} if the log is empty.
     */
    public long earliestOffset() {
        return events.isEmpty() ? nextOffset.get() : events.firstKey();
    }

    /** Returns the next offset that will be assigned to the next appended event. */
    public long latestOffset() {
        return nextOffset.get();
    }

    public long size() {
        return events.size();
    }

    public long estimatedBytes() {
        return estimatedBytes.get();
    }

    /**
     * Evicts events that violate the retention policy.
     * Eviction is oldest-first (lowest offset first).
     */
    public void applyRetention() {
        long now = System.currentTimeMillis();
        long cutoffTime = now - retention.getMaxAge().toMillis();

        int evicted = 0;

        for (var entry : events.entrySet()) {
            boolean tooOld = entry.getValue().getTimestamp() < cutoffTime;
            boolean tooMany = events.size() - evicted > retention.getMaxEventsPerPartition();
            boolean tooLarge = estimatedBytes.get() > retention.getMaxBytesPerPartition();

            if (tooOld || tooMany || tooLarge) {
                Event removed = events.remove(entry.getKey());
                if (removed != null) {
                    estimatedBytes.addAndGet(-estimateSize(removed));
                    evicted++;
                }
            } else {
                break; // Events are ordered by offset (which is chronological), so we can stop early
            }
        }

        if (evicted > 0) {
            log.debug("Retention evicted {} events from {}-{}", evicted, topic, partition);
        }
    }

    public void clear() {
        events.clear();
        nextOffset.set(0);
        estimatedBytes.set(0);
    }

    private long estimateSize(Event event) {
        return event.getPayload() != null ? event.getPayload().length() : 0;
    }
}
