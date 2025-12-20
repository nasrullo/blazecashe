package main

import (
	"fmt"
	"log"

	blazecache "github.com/blazecache/client"
)

func main() {
	fmt.Println("Testing BlazeCache Go Client - PUT command")

	// Create client
	client, err := blazecache.New("127.0.0.1:6784")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test PUT (Set)
	fmt.Println("\nTesting Set (PUT)...")
	key := "test-put-key"
	value := []byte("test-put-value")

	if err := client.Set(key, value, 0); err != nil {
		fmt.Printf("✗ Set failed: %v\n", err)
		log.Fatal(err)
	} else {
		fmt.Printf("✓ Set successful: key='%s', value='%s'\n", key, string(value))
	}

	// Verify with GET
	fmt.Println("\nVerifying with Get...")
	result, err := client.Get(key)
	if err != nil {
		if err == blazecache.ErrNotFound {
			fmt.Println("✗ Key not found after Set")
		} else {
			fmt.Printf("✗ Get failed: %v\n", err)
		}
	} else {
		if string(result) == string(value) {
			fmt.Printf("✓ Get successful: value matches '%s'\n", string(result))
		} else {
			fmt.Printf("✗ Value mismatch: expected '%s', got '%s'\n", string(value), string(result))
		}
	}

	fmt.Println("\n✓ PUT command test completed successfully!")
}
