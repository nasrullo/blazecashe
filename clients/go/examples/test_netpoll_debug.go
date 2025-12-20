package main

import (
	"fmt"
	"log"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	fmt.Println("Testing netpoll connection...")

	// Create client
	client, err := blazecache.New("127.0.0.1:6784")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test Ping first
	fmt.Println("Testing Ping...")
	if err := client.Ping(); err != nil {
		fmt.Printf("Ping failed: %v\n", err)
		log.Fatal(err)
	}
	fmt.Println("✓ Ping successful")

	// Test Set with timeout
	fmt.Println("Testing Set with 5 second timeout...")
	key := "test-key"
	value := []byte("test-value")
	
	done := make(chan bool)
	go func() {
		if err := client.Set(key, value, 0); err != nil {
			fmt.Printf("Set failed: %v\n", err)
		} else {
			fmt.Println("✓ Set successful")
		}
		done <- true
	}()

	select {
	case <-done:
		fmt.Println("Set completed")
	case <-time.After(10 * time.Second):
		fmt.Println("✗ Set timed out after 10 seconds")
	}
}



