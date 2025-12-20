package main

import (
	"fmt"
	"log"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	fmt.Println("Debugging Set operation...")

	// Create client
	client, err := blazecache.New("127.0.0.1:6784")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test Ping first
	fmt.Println("1. Testing Ping...")
	if err := client.Ping(); err != nil {
		fmt.Printf("   Ping failed: %v\n", err)
		log.Fatal(err)
	}
	fmt.Println("   ✓ Ping successful")

	// Test Set
	fmt.Println("2. Testing Set...")
	key := "debug-key"
	value := []byte("debug-value")
	
	start := time.Now()
	if err := client.Set(key, value, 0); err != nil {
		fmt.Printf("   Set failed after %v: %v\n", time.Since(start), err)
		log.Fatal(err)
	}
	fmt.Printf("   ✓ Set successful in %v\n", time.Since(start))

	// Test Get to verify
	fmt.Println("3. Testing Get to verify...")
	result, err := client.Get(key)
	if err != nil {
		fmt.Printf("   Get failed: %v\n", err)
	} else {
		if string(result) == string(value) {
			fmt.Printf("   ✓ Get successful: value matches\n")
		} else {
			fmt.Printf("   ✗ Value mismatch: expected '%s', got '%s'\n", string(value), string(result))
		}
	}
}



