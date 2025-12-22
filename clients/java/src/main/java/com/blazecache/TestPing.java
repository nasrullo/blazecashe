package com.blazecache;

import java.io.IOException;

public class TestPing {
    public static void main(String[] args) {
        try (UDPClient client = new UDPClient("127.0.0.1:6793")) {
            System.out.println("Sending PING to 127.0.0.1:6793...");
            client.ping();
            System.out.println("✓ PING successful!");
        } catch (IOException e) {
            System.err.println("✗ PING failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

