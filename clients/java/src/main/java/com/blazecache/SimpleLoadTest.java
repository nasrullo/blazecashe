package com.blazecache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleLoadTest {
    public static void main(String[] args) {
        List<String> servers = Arrays.asList(
            "127.0.0.1:6784",
            "127.0.0.1:6786",
            "127.0.0.1:6788"
        );

        if (args.length > 0) {
            servers = Arrays.asList(args[0].split(","));
        }

        int ops = 100000;
        if (args.length > 1) {
            try {
                ops = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid ops: " + args[1]);
            }
        }

        CacheClient client = new CacheClient(servers, CacheClient.SelectionStrategy.ROUND_ROBIN);

        // Wait for servers to be ready
        for (int i = 0; i < 30; i++) {
            try {
                client.ping();
                break;
            } catch (IOException e) {
                if (i == 29) {
                    System.out.println("❌ Servers not ready after 30 seconds");
                    return;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        AtomicLong ok = new AtomicLong(0);
        AtomicLong errs = new AtomicLong(0);
        long start = System.nanoTime();

        for (int i = 0; i < ops; i++) {
            String key = "load-key-" + i;
            byte[] val = ("value-" + i).getBytes();

            try {
                client.set(key, val);
            } catch (IOException e) {
                System.err.printf("SET error on %s: %s%n", key, e.getMessage());
                errs.incrementAndGet();
                continue;
            }

            try {
                Optional<byte[]> result = client.get(key);
                if (result.isPresent() && Arrays.equals(result.get(), val)) {
                    ok.incrementAndGet();
                } else {
                    System.err.printf("Data mismatch on %s%n", key);
                    errs.incrementAndGet();
                }
            } catch (IOException e) {
                System.err.printf("GET error on %s: %s%n", key, e.getMessage());
                errs.incrementAndGet();
            }
        }

        long duration = System.nanoTime() - start;
        double durationSeconds = duration / 1_000_000_000.0;
        System.out.printf("Load test complete. total_ops=%d success=%d errors=%d duration=%.3fs%n",
            ops, ok.get(), errs.get(), durationSeconds);
    }
}
