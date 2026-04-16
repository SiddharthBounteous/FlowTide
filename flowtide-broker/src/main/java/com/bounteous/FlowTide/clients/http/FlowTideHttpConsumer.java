package com.bounteous.FlowTide.clients.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.OffsetCommitRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * HTTP consumer client — polls events from FlowTide over REST.
 *
 * <p>Replaces direct TCP usage for external applications. Internally calls:
 * <ul>
 *   <li>{@code GET /api/consumer/groups/{groupId}/poll}    — consumer group poll
 *   <li>{@code POST /api/consumer/offsets/commit}          — manual offset commit
 *   <li>{@code GET /api/consumer/groups/{groupId}/offsets} — get committed offsets
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * FlowTideHttpConsumer consumer = new FlowTideHttpConsumer("http://localhost:8080", "my-group", "member-1");
 * List&lt;Event&gt; events = consumer.poll("orders");
 * </pre>
 */
public class FlowTideHttpConsumer {

    private static final Logger log = LoggerFactory.getLogger(FlowTideHttpConsumer.class);

    private static final String POLL_PATH          = "/api/consumer/groups/%s/poll";
    private static final String COMMIT_PATH        = "/api/consumer/offsets/commit";
    private static final String OFFSETS_PATH       = "/api/consumer/groups/%s/offsets";

    private final String baseUrl;
    private final String groupId;
    private final String memberId;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public FlowTideHttpConsumer(String baseUrl, String groupId, String memberId) {
        this.baseUrl  = baseUrl.stripTrailing().replaceAll("/$", "");
        this.groupId  = groupId;
        this.memberId = memberId;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.mapper = new ObjectMapper();
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Poll
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Polls for new events on the given topic using COMMITTED offset strategy.
     * Auto-commits the offset after reading.
     *
     * @param topic topic to consume from
     * @return list of events (empty if nothing new)
     * @throws FlowTideHttpException on HTTP error
     */
    public List<Event> poll(String topic) {
        return poll(topic, "COMMITTED", 100, true);
    }

    /**
     * Polls with full control over offset strategy, limit, and auto-commit.
     *
     * @param topic        topic to consume from
     * @param strategy     COMMITTED, EARLIEST, or LATEST
     * @param limit        max number of events to return
     * @param autoCommit   if true, offset is committed automatically after poll
     * @return list of events
     * @throws FlowTideHttpException on HTTP error
     */
    public List<Event> poll(String topic, String strategy, int limit, boolean autoCommit) {
        try {
            String url = baseUrl + String.format(POLL_PATH, encode(groupId))
                    + "?topic="      + encode(topic)
                    + "&memberId="   + encode(memberId)
                    + "&strategy="   + encode(strategy)
                    + "&limit="      + limit
                    + "&autoCommit=" + autoCommit;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .timeout(Duration.ofSeconds(10))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new FlowTideHttpException("Poll failed: " + response.statusCode() + " — " + response.body(), response.statusCode());
            }

            List<Event> events = mapper.readValue(response.body(), new TypeReference<>() {});
            log.debug("Polled {} events from topic={} group={}", events.size(), topic, groupId);
            return events;

        } catch (FlowTideHttpException e) {
            throw e;
        } catch (Exception e) {
            throw new FlowTideHttpException("Failed to poll events: " + e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Offset management
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Manually commits an offset for a specific topic-partition.
     * Only needed when polling with {@code autoCommit=false}.
     *
     * @param topic     topic name
     * @param partition partition index
     * @param offset    next offset to read from on the following poll
     * @throws FlowTideHttpException on HTTP error
     */
    public void commitOffset(String topic, int partition, long offset) {
        try {
            OffsetCommitRequest commitRequest = new OffsetCommitRequest();
            commitRequest.setGroupId(groupId);
            commitRequest.setTopic(topic);
            commitRequest.setPartition(partition);
            commitRequest.setOffset(offset);

            String body = mapper.writeValueAsString(commitRequest);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + COMMIT_PATH))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(5))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new FlowTideHttpException("Commit failed: " + response.statusCode() + " — " + response.body(), response.statusCode());
            }

            log.debug("Committed offset: group={} topic={} partition={} offset={}", groupId, topic, partition, offset);

        } catch (FlowTideHttpException e) {
            throw e;
        } catch (Exception e) {
            throw new FlowTideHttpException("Failed to commit offset: " + e.getMessage(), e);
        }
    }

    /**
     * Returns all committed offsets for this consumer group.
     * Key format: "topic:partition"
     *
     * @return map of partition key to committed offset
     * @throws FlowTideHttpException on HTTP error
     */
    public Map<String, Long> getCommittedOffsets() {
        try {
            String url = baseUrl + String.format(OFFSETS_PATH, encode(groupId));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .timeout(Duration.ofSeconds(5))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new FlowTideHttpException("Get offsets failed: " + response.statusCode() + " — " + response.body(), response.statusCode());
            }

            return mapper.readValue(response.body(), new TypeReference<>() {});

        } catch (FlowTideHttpException e) {
            throw e;
        } catch (Exception e) {
            throw new FlowTideHttpException("Failed to get offsets: " + e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Internal
    // ─────────────────────────────────────────────────────────────────────────

    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
