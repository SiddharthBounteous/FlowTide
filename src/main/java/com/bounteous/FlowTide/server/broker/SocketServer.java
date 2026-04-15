package com.bounteous.FlowTide.server.broker;

import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SocketServer {

    private static final Logger log = LoggerFactory.getLogger(SocketServer.class);

    private final int port;
    private final LogManager logManager;
    private final ExecutorService threadPool;
    private volatile boolean running = true;

    public SocketServer(int port, LogManager logManager, int threadPoolSize) {
        this.port = port;
        this.logManager = logManager;
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize,
                r -> new Thread(r, "broker-handler-" + System.nanoTime()));
    }

    public void start() {
        log.info("Broker started on port {}", port);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (running) {
                Socket socket = serverSocket.accept();
                log.debug("Connection received from {}", socket.getRemoteSocketAddress());

                threadPool.submit(() -> {
                    try {
                        socket.setSoTimeout(5000);
                        new RequestHandler(logManager).handle(socket);
                    } catch (Exception e) {
                        log.error("Error handling request from {}", socket.getRemoteSocketAddress(), e);
                    } finally {
                        try { socket.close(); } catch (Exception ignored) {}
                    }
                });
            }

        } catch (Exception e) {
            if (running) {
                log.error("Broker socket server error on port {}", port, e);
            }
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        running = false;
        log.info("Shutting down broker on port {}", port);
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
