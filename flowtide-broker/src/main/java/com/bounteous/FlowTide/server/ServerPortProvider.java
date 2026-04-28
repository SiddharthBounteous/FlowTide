package com.bounteous.FlowTide.server;

import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * Captures the actual HTTP port after Tomcat binds.
 *
 * <p>When {@code server.port=0}, Spring picks a random free port.
 * {@code @Value("${server.port}")} would return {@code 0} (the configured
 * value), not the real port.  This listener receives
 * {@link WebServerInitializedEvent} — fired by Spring Boot after the web
 * server has started — and stores the actual port for use by any bean that
 * needs to advertise itself (registration, heartbeat, etc.).
 */
@Component
public class ServerPortProvider implements ApplicationListener<WebServerInitializedEvent> {

    private volatile int port = 0;

    @Override
    public void onApplicationEvent(WebServerInitializedEvent event) {
        this.port = event.getWebServer().getPort();
    }

    /** Returns the port Tomcat is actually listening on (never 0 after startup). */
    public int getPort(){
        return port;
    }
}
