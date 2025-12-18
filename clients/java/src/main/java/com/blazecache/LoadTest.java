package com.blazecache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class LoadTest {
    public static void main(String[] args) {
        int totalPuts = 100000;
        int concurrency = 32;
        int progressStep = 10000;

        if (args.length > 0) {
            try {
                totalPuts = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid totalPuts: " + args[0]);
            }
        }
        if (args.length > 1) {
            try {
                concurrency = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid concurrency: " + args[1]);
            }
        }
        if (args.length > 2) {
            try {
                progressStep = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid progressStep: " + args[2]);
            }
        }

        CacheClient.SelectionStrategy strategy = CacheClient.SelectionStrategy.CONSISTENT_HASHING;
        if (args.length > 3) {
            if ("rr".equalsIgnoreCase(args[3])) {
                strategy = CacheClient.SelectionStrategy.ROUND_ROBIN;
            } else if ("hash".equalsIgnoreCase(args[3])) {
                strategy = CacheClient.SelectionStrategy.CONSISTENT_HASHING;
            }
        }

        // Get server addresses from environment variable or use default
        List<String> servers;
        String serversEnv = System.getenv("BLAZECACHE_SERVERS");
        if (serversEnv != null && !serversEnv.isEmpty()) {
            // Parse comma-separated server list from environment
            servers = Arrays.asList(serversEnv.split(","));
        } else {
            // Detect if running in Docker and use host.docker.internal
            // Otherwise use 127.0.0.1 for local development
            String host = detectHost();
            servers = Arrays.asList(
                host + ":6784",
                host + ":6786",
                host + ":6788"
            );
        }

        // Use peer discovery for consistent hashing, static list for round robin
        CacheClient client;
        if (strategy == CacheClient.SelectionStrategy.CONSISTENT_HASHING) {
            // Use peer discovery with 5-second refresh interval
            // Client will automatically discover all peers and update hash ring
            String seedServer = servers.get(0);
            client = CacheClient.withDiscovery(seedServer, 5);
            
            // Wait a moment for initial peer discovery to complete
            try {
                Thread.sleep(2000); // Give discovery time to fetch peer list
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            // Round robin doesn't need discovery - use static server list
            client = new CacheClient(servers, strategy);
        }

        AtomicLong ok = new AtomicLong(0);
        AtomicLong fail = new AtomicLong(0);
        long start = System.nanoTime();

        // Use virtual threads with semaphore for rate limiting to prevent connection storms
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        // Limit concurrent operations to prevent overwhelming the server
        // For larger loads, allow more concurrency; for smaller loads, no limit needed
        int semaphoreLimit = totalPuts > 10000 
            ? Math.min(concurrency * 10, 500)  // Higher limit for large loads (320-500)
            : Integer.MAX_VALUE;  // No limit for small loads (<10K)
        java.util.concurrent.Semaphore semaphore = new java.util.concurrent.Semaphore(semaphoreLimit);
        CountDownLatch latch = new CountDownLatch(totalPuts);
        final int finalTotalPuts = totalPuts;
        final int finalProgressStep = progressStep;
        AtomicLong completedCount = new AtomicLong(0);

        for (int i = 0; i < totalPuts; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    // Acquire permit to limit concurrent operations
                    semaphore.acquire();
                    try {
                        String key = "load_put_" + idx;
                        client.set(key, "value".getBytes());
                        ok.incrementAndGet();
                    } finally {
                        semaphore.release();
                    }
                } catch (IOException e) {
                    // Only log first few failures to avoid spam
                    long failCount = fail.get();
                    if (failCount < 10) {
                        System.err.println("put load_put_" + idx + " failed: " + e.getMessage());
                    }
                    fail.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail.incrementAndGet();
                } finally {
                    latch.countDown();
                    
                    // Progress reporting
                    long completed = completedCount.incrementAndGet();
                    if (completed % finalProgressStep == 0) {
                        System.out.println("progress: " + completed + " / " + finalTotalPuts);
                    }
                }
            });
        }

        try {
            latch.await();
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Interrupted: " + e.getMessage());
        } finally {
            // Stop peer discovery when load test completes (if enabled)
            if (strategy == CacheClient.SelectionStrategy.CONSISTENT_HASHING) {
                // client.stopDiscovery(); // Method not available in current implementation
            }
        }

        long duration = System.nanoTime() - start;
        double durationSeconds = duration / 1_000_000_000.0;
        long okVal = ok.get();
        long failVal = fail.get();
        double opsPerSec = okVal / durationSeconds;

        System.out.printf("puts ok: %d fail: %d total: %d in %.3fs (%.1f ops/sec)%n",
            okVal, failVal, okVal + failVal, durationSeconds, opsPerSec);
    }
    
    /**
     * Detect the appropriate host to connect to.
     * When running in Docker with --network host, use 127.0.0.1.
     * When running in Docker normally, use host.docker.internal.
     * Otherwise, use 127.0.0.1 for local development.
     */
    private static String detectHost() {
        // Check if we're in Docker by looking for common indicators
        boolean inDocker = System.getenv("container") != null ||
                          new java.io.File("/.dockerenv").exists();
        
        if (inDocker) {
            // Check if we're using --network host by trying to connect to 127.0.0.1
            // If we can resolve it, we're likely using host networking
            try {
                java.net.InetAddress.getByName("127.0.0.1");
                // If we're using --network host, 127.0.0.1 will work
                // Try a quick test connection (non-blocking check)
                return "127.0.0.1";
            } catch (Exception e) {
                // Fall through to host.docker.internal
            }
            
            // Use host.docker.internal for regular Docker networking
            // Note: This requires Docker Desktop or Docker with special configuration
            // For production, consider using environment variable BLAZECACHE_SERVERS
            return "host.docker.internal";
        }
        
        // Local development - use localhost
        return "127.0.0.1";
    }
}

