package com.bounteous.FlowTide.consumer;

import com.bounteous.FlowTide.client.CoordinatorClient;
import com.bounteous.FlowTide.model.TopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local cache of partition→leader mappings fetched from flowtide-controller.
 *
 * <h3>Why this exists</h3>
 * Without this cache, every poll goes through Eureka load balancing and hits a
 * random broker.  Two-thirds of those hits land on the wrong broker, which then
 * has to ask the controller for the leader and forward the request — adding two
 * extra network hops per wrong hit.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>On {@code join}, the consumer calls {@link #refresh(String)} to populate
 *       the leader map for that topic.
 *   <li>On every {@code poll}, the consumer calls {@link #getLeaderUrl(String, int)}
 *       to get the direct {@code http://host:port} for the target partition's leader.
 *   <li>If a broker returns HTTP 409 (it is no longer the leader after a failover),
 *       the consumer calls {@link #refresh(String)} and retries — this is the only
 *       time the cache is invalidated.
 * </ol>
 *
 * <h3>Result</h3>
 * Under normal operation (no failover), every fetch goes directly to the correct
 * leader broker with zero extra hops and zero controller load per poll.
 */
@Service
public class LeaderMetadataCache {

    private static final Logger log = LoggerFactory.getLogger(LeaderMetadataCache.class);

    /** topic → { partition → "host:port" } */
    private final ConcurrentHashMap<String, Map<Integer, String>> cache = new ConcurrentHashMap<>();

    private final CoordinatorClient coordinatorClient;

    public LeaderMetadataCache(CoordinatorClient coordinatorClient) {
        this.coordinatorClient = coordinatorClient;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Public API
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the direct base URL ({@code "http://host:port"}) of the leader
     * broker for the given topic-partition, or {@code null} if unknown.
     */
    public String getLeaderUrl(String topic, int partition) {
        Map<Integer, String> leaderMap = cache.get(topic);
        if (leaderMap == null) return null;
        String brokerId = leaderMap.get(partition);
        return brokerId != null ? "http://" + brokerId : null;
    }

    /**
     * Forces a fresh fetch of the leader map for {@code topic} from the controller.
     * Call this on startup (join) and on any 409 response (leader changed).
     */
    public void refresh(String topic) {
        try {
            TopicMetadata meta = coordinatorClient.getTopicMetadata(topic);
            if (meta != null && meta.getLeaderMap() != null && !meta.getLeaderMap().isEmpty()) {
                cache.put(topic, meta.getLeaderMap());
                log.info("Leader cache refreshed for topic '{}': {}", topic, meta.getLeaderMap());
            } else {
                log.warn("Controller returned empty leader map for topic '{}' — cache unchanged", topic);
            }
        } catch (Exception e) {
            log.warn("Could not refresh leader cache for topic '{}': {}", topic, e.getMessage());
        }
    }

    /**
     * Refreshes the cache only if no entry exists yet for {@code topic}.
     * Used as a lazy-init guard — avoids duplicate controller calls if
     * refresh was already done at join time.
     */
    public void refreshIfAbsent(String topic) {
        if (!cache.containsKey(topic)) {
            refresh(topic);
        }
    }

    /** Removes the cached entry for {@code topic} (e.g. topic deleted). */
    public void evict(String topic) {
        cache.remove(topic);
    }
}
