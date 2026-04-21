package com.bounteous.FlowTide.client;

import com.bounteous.FlowTide.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;
import java.util.List;

/**
 * Makes direct HTTP calls to a specific broker by explicit base URL.
 *
 * <p>Unlike {@link BrokerClient} (which uses Feign + Eureka load balancing
 * and can hit any random broker), this client targets the <em>exact</em>
 * leader broker for a partition — resolved from {@code LeaderMetadataCache}.
 *
 * <p>A plain {@link RestTemplate} is used intentionally: Feign/LoadBalancer
 * must not intercept these calls because the URL is already the correct target.
 *
 * <h3>Error contract</h3>
 * <ul>
 *   <li>HTTP 409 — the target broker is no longer the leader (failover happened).
 *       The caller should refresh the leader cache and retry once.
 *   <li>Other errors — propagated as {@link org.springframework.web.client.RestClientException}.
 * </ul>
 */
@Service
public class DirectBrokerClient {

    private static final Logger log = LoggerFactory.getLogger(DirectBrokerClient.class);

    /** Plain RestTemplate — no Eureka, no load balancing. URL is explicit. */
    private final RestTemplate restTemplate = new RestTemplate();

    // ─────────────────────────────────────────────────────────────────────────
    //  Fetch events
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Fetches events from a specific leader broker.
     *
     * @param baseUrl   direct broker URL, e.g. {@code "http://localhost:8083"}
     * @param topic     topic name
     * @param partition partition index
     * @param offset    start offset (inclusive)
     * @param limit     maximum number of events to return
     * @return list of events; empty list if none available
     */
    public List<Event> fetchEvents(String baseUrl, String topic, int partition,
                                   long offset, int limit) {
        String url = UriComponentsBuilder
                .fromHttpUrl(baseUrl + "/internal/events/{topic}/{partition}")
                .queryParam("offset", offset)
                .queryParam("limit", limit)
                .buildAndExpand(topic, partition)
                .toUriString();

        log.debug("Direct fetch → {} (offset={} limit={})", url, offset, limit);

        ResponseEntity<List<Event>> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});

        List<Event> body = response.getBody();
        return body != null ? body : Collections.emptyList();
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Latest offset (high-water mark)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the current high-water mark (latest offset) for a partition
     * directly from its leader broker.
     *
     * @param baseUrl   direct broker URL, e.g. {@code "http://localhost:8083"}
     * @param topic     topic name
     * @param partition partition index
     * @return latest offset, or {@code 0} if the broker returned no body
     */
    public long getLatestOffset(String baseUrl, String topic, int partition) {
        String url = baseUrl + "/internal/replicate/" + topic + "/" + partition + "/offset";

        log.debug("Direct latestOffset → {}", url);

        ResponseEntity<Long> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                Long.class);

        Long body = response.getBody();
        return body != null ? body : 0L;
    }
}
