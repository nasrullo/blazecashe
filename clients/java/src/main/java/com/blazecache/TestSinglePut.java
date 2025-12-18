package com.blazecache;

import java.util.Arrays;
import java.util.List;

public class TestSinglePut {
    public static void main(String[] args) {
        List<String> servers = Arrays.asList(
            "127.0.0.1:6784",
            "127.0.0.1:6786",
            "127.0.0.1:6788"
        );

        System.out.println("Creating client...");
        CacheClient client = new CacheClient(servers, CacheClient.SelectionStrategy.CONSISTENT_HASHING);

        try {
            System.out.println("Testing ping...");
            client.ping();
            System.out.println("✓ Ping successful");

            System.out.println("Testing PUT...");
            client.set("test_key_1", "test_value_1".getBytes());
            System.out.println("✓ PUT successful");

            System.out.println("Testing GET...");
            java.util.Optional<byte[]> result = client.get("test_key_1");
            if (result.isPresent()) {
                System.out.println("✓ GET successful: key=test_key_1, value=" + new String(result.get()));
            } else {
                System.out.println("✗ GET returned empty");
            }

            System.out.println("\n✅ Single PUT test completed successfully!");
        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

