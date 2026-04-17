package com.bounteous.FlowTide.partition;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;

/**
 * Forwards produce/fetch requests to the broker that is the leader for a
 * given topic-partition, when the current broker is not the leader.
 *
 * <p>The leader's address is resolved via flowtide-controller
 * ({@link ControllerClient#getLeader}).  The forwarded call is a plain HTTP
 * request to the target broker's {@code /internal} endpoints.
 *
 * <p>A dedicated (non-load-balanced) {@link RestTemplate} is used here
 * because the target URL is dynamic and resolved at runtime.
 */
@Service
public class BrokerForwardingService {

    private static final Logger log = LoggerFactory.getLogger(BrokerForwardingService.class);

    private final ControllerClient controllerClient;
    private final RestTemplate     restTemplate;

    public BrokerForwardingService(ControllerClient controllerClient) {
        this.controllerClient = controllerClient;
        this.restTemplate     = new RestTemplate();   // plain, no Eureka LB — URL is explicit
    }

    // -----------------------------------------------------------------------
    // Produce
    // -----------------------------------------------------------------------

    /**
     * Forwards a publish request to the leader broker for the given
     * topic-partition.
     *
     * @param topic     target topic
     * @param partition target partition
     * @param request   original publish request
     * @return the publish response from the leader broker
     * @throws PartitionRoutingException if the leader cannot be resolved
     */
    public PublishResponse forwardPublish(String topic, int partition, PublishRequest request) {
        String leaderUrl = resolveLeaderBaseUrl(topic, partition);
        String endpoint  = leaderUrl + "/internal/events";

        log.debug("Forwarding publish → {} (topic={} partition={})", endpoint, topic, partition);

        ResponseEntity<PublishResponse> response = restTemplate.exchange(
                endpoint,
                HttpMethod.POST,
                new HttpEntity<>(request),
                PublishResponse.class);

        return response.getBody();
    }

    // -----------------------------------------------------------------------
    // Fetch
    // -----------------------------------------------------------------------

    /**
     * Forwards a fetch request to the leader broker.
     *
     * @param topic     target topic
     * @param partition target partition
     * @param offset    start offset
     * @param limit     max events to return
     * @return events from the leader broker
     */
    public List<Event> forwardFetch(String topic, int partition, long offset, int limit) {
        String leaderUrl = resolveLeaderBaseUrl(topic, partition);

        String url = UriComponentsBuilder
                .fromHttpUrl(leaderUrl + "/internal/events/{topic}/{partition}")
                .queryParam("offset", offset)
                .queryParam("limit", limit)
                .buildAndExpand(topic, partition)
                .toUriString();

        log.debug("Forwarding fetch → {} (offset={} limit={})", url, offset, limit);

        ResponseEntity<List<Event>> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});

        return response.getBody();
    }

    // -----------------------------------------------------------------------
    // Leader resolution
    // -----------------------------------------------------------------------

    /**
     * Asks flowtide-controller for the leader broker ID ({@code "host:port"})
     * and converts it to an HTTP base URL ({@code "http://host:port"}).
     */
    private String resolveLeaderBaseUrl(String topic, int partition) {
        String brokerId;
        try {
            brokerId = controllerClient.getLeader(topic, partition);
        } catch (Exception e) {
            throw new PartitionRoutingException(
                    "Could not resolve leader for " + topic + "-" + partition, e);
        }

        if (brokerId == null || brokerId.isBlank()) {
            throw new PartitionRoutingException(
                    "Controller returned no leader for " + topic + "-" + partition);
        }

        // brokerId format: "host:port"
        return "http://" + brokerId;
    }
}
