package com.blazecache;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

public class TestTLSClient {
    public static void main(String[] args) {
        String serverAddr = "localhost:8443";
        if (args.length > 0) {
            serverAddr = args[0];
        }

        System.out.println("Connecting to TLS server at " + serverAddr + "...");
        
        // Wait a bit for server to be ready
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            // Create TLS client (insecure mode for self-signed cert)
            TLSCacheClient client = new TLSCacheClient(
                Arrays.asList(serverAddr),
                TLSCacheClient.SelectionStrategy.ROUND_ROBIN,
                false // disable certificate verification for self-signed cert
            );

            System.out.println("Testing TLS client operations...");

            // Test PUT
            String key = "test-key";
            byte[] value = "test-value".getBytes();
            client.set(key, value);
            System.out.println("✓ PUT successful");

            // Test GET
            Optional<byte[]> result = client.get(key);
            if (result.isEmpty()) {
                System.err.println("✗ GET failed: returned empty");
                System.exit(1);
            }
            if (!Arrays.equals(result.get(), value)) {
                System.err.println("✗ GET returned wrong value");
                System.exit(1);
            }
            System.out.println("✓ GET successful: " + new String(result.get()));

            // Test DELETE
            boolean deleted = client.delete(key);
            if (!deleted) {
                System.err.println("✗ DELETE failed");
                System.exit(1);
            }
            System.out.println("✓ DELETE successful");

            // Verify DELETE worked
            result = client.get(key);
            if (result.isPresent()) {
                System.err.println("✗ GET after DELETE should have returned empty");
                System.exit(1);
            }
            System.out.println("✓ GET after DELETE correctly returned not found");

            System.out.println("\n✅ All TLS tests passed!");

        } catch (IOException e) {
            System.err.println("✗ Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

