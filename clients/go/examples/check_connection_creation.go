package main

import (
	"fmt"
	"log"
	"time"
	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := "127.0.0.1:6792"
	
	fmt.Printf("=== Connection Creation Time Analysis ===\n")
	fmt.Printf("Server: %s\n\n", serverAddr)
	
	// Test ping first to verify server
	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	
	if err := client.Ping(); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("✓ Server connection verified\n")
	
	// Measure connection creation time
	// Note: This is tricky because the client pools connections
	// We'll measure the time for first operation vs subsequent operations
	
	fmt.Println("=== First Operation (may create connection) ===")
	start1 := time.Now()
	item := &blazecache.Item{Key: "test-key-1", Value: []byte("test-value-1")}
	if err := client.Set(item); err != nil {
		log.Fatalf("Set failed: %v", err)
	}
	firstOpTime := time.Since(start1)
	fmt.Printf("First SET operation: %s\n", firstOpTime)
	
	fmt.Println("\n=== Subsequent Operations (should reuse connection) ===")
	var times []time.Duration
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-key-%d", i+2)
		value := []byte(fmt.Sprintf("test-value-%d", i+2))
		item := &blazecache.Item{Key: key, Value: value}
		
		start := time.Now()
		if err := client.Set(item); err != nil {
			log.Fatalf("Set failed: %v", err)
		}
		times = append(times, time.Since(start))
	}
	
	var sum time.Duration
	for _, t := range times {
		sum += t
	}
	avg := sum / time.Duration(len(times))
	
	fmt.Printf("Average of 100 subsequent SET operations: %s\n", avg)
	fmt.Printf("First operation overhead: %s\n", firstOpTime-avg)
	
	if firstOpTime > avg*2 {
		fmt.Println("\n⚠️  WARNING: First operation is significantly slower!")
		fmt.Println("   This suggests connection creation overhead.")
	} else {
		fmt.Println("\n✓ First operation is similar to subsequent operations.")
		fmt.Println("  Connection pooling appears to be working.")
	}
}

