package com.bounteous.FlowTide.metrics;

import com.bounteous.FlowTide.server.log.LogManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and exposes operational metrics via Micrometer.
 *
 * <p>Metrics are exposed at {@code /actuator/metrics} and can be scraped by
 * Prometheus or any Kubernetes HPA-compatible metrics adapter.
 *
 * <p>Key metrics:
 * <ul>
 *   <li>{@code kafka.events.published} — total events published per topic
 *   <li>{@code kafka.events.consumed} — total events consumed per topic/group
 *   <li>{@code kafka.storage.events.total} — current total events in memory
 *   <li>{@code kafka.storage.bytes.estimated} — current memory usage estimate
 * </ul>
 */
@Service
public class MetricsService {

    private final LogManager logManager;
    private final MeterRegistry meterRegistry;

    /** topic → publish counter */
    private final Map<String, Counter> publishCounters = new ConcurrentHashMap<>();

    /** "topic:group" → consume counter */
    private final Map<String, Counter> consumeCounters = new ConcurrentHashMap<>();

    /** topic → consumer lag value */
    private final Map<String, AtomicLong> consumerLags = new ConcurrentHashMap<>();

    public MetricsService(LogManager logManager, MeterRegistry meterRegistry) {
        this.logManager = logManager;
        this.meterRegistry = meterRegistry;

        // Register live storage gauges (auto-updated each scrape)
        Gauge.builder("kafka.storage.events.total", logManager, LogManager::totalEventsStored)
                .description("Total events currently stored in memory across all partitions")
                .register(meterRegistry);

        Gauge.builder("kafka.storage.bytes.estimated", logManager, LogManager::totalBytesEstimated)
                .description("Estimated bytes used by all partition logs")
                .register(meterRegistry);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Record events
    // ─────────────────────────────────────────────────────────────────────────

    public void recordPublish(String topic, int partition) {
        publishCounters.computeIfAbsent(topic, t ->
                Counter.builder("kafka.events.published")
                        .tag("topic", t)
                        .description("Number of events published to this topic")
                        .register(meterRegistry)
        ).increment();
    }

    public void recordConsume(String topic, String groupId, int count) {
        String key = topic + ":" + groupId;
        consumeCounters.computeIfAbsent(key, k ->
                Counter.builder("kafka.events.consumed")
                        .tag("topic", topic)
                        .tag("group", groupId)
                        .description("Number of events consumed from this topic by this group")
                        .register(meterRegistry)
        ).increment(count);
    }

    public void updateConsumerLag(String topic, String groupId, long lag) {
        String key = topic + ":" + groupId;
        AtomicLong lagValue = consumerLags.computeIfAbsent(key, k -> {
            AtomicLong atom = new AtomicLong(0);
            Gauge.builder("kafka.consumer.lag", atom, AtomicLong::get)
                    .tag("topic", topic)
                    .tag("group", groupId)
                    .description("Number of unconsumed events for this consumer group")
                    .register(meterRegistry);
            return atom;
        });
        lagValue.set(lag);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Snapshot for Admin API
    // ─────────────────────────────────────────────────────────────────────────

    public MetricsSnapshot snapshot() {
        Map<String, Long> eventsPerTopic = new java.util.LinkedHashMap<>();
        Map<String, Long> bytesPerTopic  = new java.util.LinkedHashMap<>();

        for (String topic : logManager.getAllTopics()) {
            long topicEvents = 0;
            long topicBytes  = 0;
            for (var entry : logManager.getTopicLogs(topic).entrySet()) {
                topicEvents += entry.getValue().size();
                topicBytes  += entry.getValue().estimatedBytes();
            }
            eventsPerTopic.put(topic, topicEvents);
            bytesPerTopic.put(topic, topicBytes);
        }

        long totalPartitions = logManager.getAllTopics().stream()
                .mapToLong(t -> logManager.getTopicLogs(t).size())
                .sum();

        long publishTotal = publishCounters.values().stream()
                .mapToLong(c -> (long) c.count())
                .sum();

        return MetricsSnapshot.builder()
                .totalEventsStored(logManager.totalEventsStored())
                .totalBytesEstimated(logManager.totalBytesEstimated())
                .topicCount(logManager.getAllTopics().size())
                .totalPartitions((int) totalPartitions)
                .eventsPerTopic(eventsPerTopic)
                .bytesPerTopic(bytesPerTopic)
                .publishedTotal(publishTotal)
                .snapshotTimestamp(System.currentTimeMillis())
                .build();
    }
}
