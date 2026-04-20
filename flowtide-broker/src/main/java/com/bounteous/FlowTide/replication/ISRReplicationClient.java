package com.bounteous.FlowTide.replication;

import com.bounteous.FlowTide.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * Pushes events from the leader to a specific follower broker synchronously.
 *
 * <p>Uses a dedicated {@link RestTemplate} with a short connect + read timeout
 * ({@code ack-timeout-ms}) so slow/dead followers are detected quickly and
 * removed from ISR without blocking the produce path for too long.
 *
 * <p>Target endpoint on the follower:
 * <pre>POST http://&lt;follower-host:port&gt;/internal/replicate/{topic}/{partition}</pre>
 */
@Component
public class ISRReplicationClient {

    private static final Logger log = LoggerFactory.getLogger(ISRReplicationClient.class);

    /**
     * Pushes a batch of events to a follower and waits for ACK.
     *
     * @param followerId  "host:port" of the follower
     * @param topic       topic name
     * @param partition   partition index
     * @param events      events to push (usually just one — the latest append)
     * @param timeoutMs   max millis to wait for ACK before declaring follower dead
     * @throws Exception  on network error or timeout — caller removes follower from ISR
     */
    public void push(String followerId, String topic, int partition,
                     List<Event> events, long timeoutMs) {

        RestTemplate rt = buildRestTemplate((int) timeoutMs);

        String url = "http://" + followerId + "/internal/replicate/" + topic + "/" + partition;

        rt.exchange(url, HttpMethod.POST, new HttpEntity<>(events), String.class);

        log.trace("Pushed {} event(s) to follower={} {}-{}",
                events.size(), followerId, topic, partition);
    }

    private RestTemplate buildRestTemplate(int timeoutMs) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeoutMs);
        factory.setReadTimeout(timeoutMs);
        return new RestTemplate(factory);
    }
}
