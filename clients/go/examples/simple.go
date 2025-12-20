package main

import (
	"fmt"
	"log"
	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := "127.0.0.1:6792"
	
	fmt.Printf("Creating client for %s...\n", serverAddr)
	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	
	fmt.Println("Testing Ping...")
	if err := client.Ping(); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("✓ Ping successful")
	
	fmt.Println("Testing Set...")
	item := &blazecache.Item{Key: "test-key", Value: []byte("test-value")}
	if err := client.Set(item); err != nil {
		log.Fatalf("Set failed: %v", err)
	}
	fmt.Println("✓ Set successful")
	
	fmt.Println("Testing Get...")
	result, err := client.Get("test-key")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	if result == nil {
		log.Fatal("Get returned nil")
	}
	if string(result.Value) != "test-value" {
		log.Fatalf("Get returned wrong value: got %s, expected test-value", string(result.Value))
	}
	fmt.Println("✓ Get successful")
	
	fmt.Println("\nAll tests passed!")
}

