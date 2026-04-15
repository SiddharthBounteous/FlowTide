package com.bounteous.FlowTide.server.broker;

import com.bounteous.FlowTide.server.log.LogManager;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectOutputStream;
import java.net.Socket;

public class Broker {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private static final int MAX_REGISTRATION_ATTEMPTS = 10;
    private static final long REGISTRATION_RETRY_DELAY_MS = 500;

    private final String registryHost;
    private final int registryPort;
    private final int brokerPort;
    private final String brokerHost;
    private final int threadPoolSize;
    private final LogManager logManager;

    public Broker(String brokerHost, int brokerPort,
                  String registryHost, int registryPort,
                  int threadPoolSize, LogManager logManager) {
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.registryHost = registryHost;
        this.registryPort = registryPort;
        this.threadPoolSize = threadPoolSize;
        this.logManager = logManager;
    }

    public void start() {
        registerWithRegistry();
        new SocketServer(brokerPort, logManager, threadPoolSize).start();
    }

    private void registerWithRegistry() {
        BrokerInfo brokerInfo = new BrokerInfo(brokerHost, brokerPort);

        for (int attempt = 1; attempt <= MAX_REGISTRATION_ATTEMPTS; attempt++) {
            try (
                Socket socket = new Socket(registryHost, registryPort);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
            ) {
                out.writeObject(brokerInfo);
                out.flush();
                log.info("Registered with registry at {}:{}", registryHost, registryPort);
                return;

            } catch (Exception e) {
                log.warn("Registry registration attempt {}/{} failed: {}", attempt, MAX_REGISTRATION_ATTEMPTS, e.getMessage());
                if (attempt < MAX_REGISTRATION_ATTEMPTS) {
                    try {
                        Thread.sleep(REGISTRATION_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        throw new IllegalStateException("Failed to register with registry after " + MAX_REGISTRATION_ATTEMPTS + " attempts");
    }
}
