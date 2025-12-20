package main

import (
	"fmt"
	"log"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	fmt.Println("Testing minimal Set operation...")

	// Create client
	client, err := blazecache.New("127.0.0.1:6792")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test with smallest possible Set
	fmt.Println("Setting key='a', value='b'...")
	start := time.Now()
	if err := client.Set("a", []byte("b"), 0); err != nil {
		fmt.Printf("Set failed after %v: %v\n", time.Since(start), err)
		log.Fatal(err)
	}
	fmt.Printf("Set succeeded in %v\n", time.Since(start))
}

