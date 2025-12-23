package com.blazecache;

import java.io.IOException;
import java.util.Optional;

/**
 * Simple test to debug UDP client issues - runs just 2 operations
 */
public class TestUDPSimple {
    public static void main(String[] args) {
        String serverAddr = args.length > 0 ? args[0] : "127.0.0.1:6793";
        
        System.out.println("=== Simple UDP Test (2 operations) ===");
        System.out.println("Server: " + serverAddr);
        System.out.println();
        
        try (UDPClient client = new UDPClient(serverAddr)) {
            // Operation 1: PUT
            System.out.println("Operation 1: PUT");
            try {
                String key1 = "test-key-1";
                byte[] value1 = "value-1".getBytes();
                System.out.println("  Sending PUT request...");
                client.set(key1, value1);
                System.out.println("  ✓ PUT successful");
            } catch (IOException e) {
                System.err.println("  ✗ PUT failed: " + e.getMessage());
                e.printStackTrace();
                return;
            }
            
            // Operation 2: GET
            System.out.println("Operation 2: GET");
            try {
                String key1 = "test-key-1";
                System.out.println("  Sending GET request...");
                Optional<byte[]> result = client.get(key1);
                if (result.isPresent()) {
                    System.out.println("  ✓ GET successful: " + new String(result.get()));
                } else {
                    System.err.println("  ✗ GET returned empty");
                }
            } catch (IOException e) {
                System.err.println("  ✗ GET failed: " + e.getMessage());
                e.printStackTrace();
                return;
            }
            
            System.out.println();
            System.out.println("✓ All 2 operations completed successfully!");
            
        } catch (IOException e) {
            System.err.println("Failed to create client: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

