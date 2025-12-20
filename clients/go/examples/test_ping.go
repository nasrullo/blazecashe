package main

import (
	"fmt"
	"log"
	blazecache "github.com/blazecache/client"
)

func main() {
	fmt.Println("Testing BlazeCache Go client...")
	
	// Create client
	client, err := blazecache.New("127.0.0.1:6784")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	
	// Test ping
	fmt.Println("Testing Ping...")
	if err := client.Ping(); err != nil {
		fmt.Printf("✗ Ping failed: %v\n", err)
		log.Fatal(err)
	} else {
		fmt.Println("✓ Ping successful - Go client is working!")
	}
}
