package main

import (
	"fmt"
	"log"
	"os"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := "localhost:8443"
	if len(os.Args) > 1 {
		serverAddr = os.Args[1]
	}

	fmt.Printf("Connecting to TLS server at %s...\n", serverAddr)

	// Wait a bit for server to be ready
	time.Sleep(2 * time.Second)

	// Create TLS client (insecure mode for self-signed cert)
	c, err := blazecache.NewTLSInsecure(serverAddr)
	if err != nil {
		log.Fatalf("Failed to create TLS client: %v", err)
	}

	// Test PING (not available in client, so we'll test GET/PUT/DELETE)
	fmt.Println("Testing TLS client operations...")

	// Test PUT
	item := &blazecache.Item{
		Key:   "test-key",
		Value: []byte("test-value"),
	}
	if err := c.Set(item); err != nil {
		log.Fatalf("PUT failed: %v", err)
	}
	fmt.Println("✓ PUT successful")

	// Test GET
	result, err := c.Get("test-key")
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	if result == nil {
		log.Fatalf("GET returned nil")
	}
	if string(result.Value) != "test-value" {
		log.Fatalf("GET returned wrong value: expected 'test-value', got '%s'", string(result.Value))
	}
	fmt.Printf("✓ GET successful: %s\n", string(result.Value))

	// Test DELETE
	if err := c.Delete("test-key"); err != nil {
		log.Fatalf("DELETE failed: %v", err)
	}
	fmt.Println("✓ DELETE successful")

	// Verify DELETE worked
	result, err = c.Get("test-key")
	if err == nil && result != nil {
		log.Fatalf("GET after DELETE should have failed or returned nil")
	}
	fmt.Println("✓ GET after DELETE correctly returned not found")

	fmt.Println("\n✅ All TLS tests passed!")
}

