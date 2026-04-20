package com.bounteous.FlowTide.replication;

import com.bounteous.FlowTide.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;

/**
 * Makes HTTP calls to a specific leader broker's replication endpoint.
 *
 * <p>Uses a plain (non-load-balanced) {@link RestTemplate} because the target
 * URL is resolved dynamically from the controller's leader map at runtime —
 * Eureka LB would route to the wrong instance.
 *
 * <p>Target endpoint:
 * <pre>GET http://&lt;leader-host:port&gt;/internal/replicate/{topic}/{partition}?fromOffset={offset}&amp;limit={limit}</pre>
 */
@Component
public class LeaderFetchClient {

    private static final Logger log = LoggerFactory.getLogger(LeaderFetchClient.class);

    /** Max events fetched per replication poll.  Tunable via property. */
    static final int FETCH_LIMIT = 500;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Fetches events from the leader starting at {@code fromOffset}.
     *
     * @param leaderId   leader's "host:port" as returned by the controller
     * @param topic      topic name
     * @param partition  partition index
     * @param fromOffset follower's current high-water mark (next expected offset)
     * @return list of events to replicate; empty list if none or on error
     */
    public List<Event> fetch(String leaderId, String topic, int partition, long fromOffset) {
        String url = "http://" + leaderId
                + "/internal/replicate/" + topic + "/" + partition
                + "?fromOffset=" + fromOffset
                + "&limit=" + FETCH_LIMIT;

        try {
            ResponseEntity<List<Event>> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<>() {});

            List<Event> body = response.getBody();
            return body != null ? body : Collections.emptyList();

        } catch (RestClientException e) {
            log.warn("Replication fetch failed [leader={} topic={} partition={} fromOffset={}]: {}",
                    leaderId, topic, partition, fromOffset, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Asks the leader for its current high-water mark (next offset to be assigned).
     * Used by the follower to detect when it has fully caught up.
     *
     * @return leader's latestOffset, or -1 on error
     */
    public long getLatestOffset(String leaderId, String topic, int partition) {
        String url = "http://" + leaderId
                + "/internal/replicate/" + topic + "/" + partition + "/offset";
        try {
            Long offset = restTemplate.getForObject(url, Long.class);
            return offset != null ? offset : -1L;
        } catch (RestClientException e) {
            log.warn("Could not fetch latest offset from leader={} {}-{}: {}",
                    leaderId, topic, partition, e.getMessage());
            return -1L;
        }
    }
}
