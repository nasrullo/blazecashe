package main

import (
	"fmt"
	"log"

	blazecache "github.com/blazecache/client"
)

func main() {
	servers := []string{
		"127.0.0.1:6784",
		"127.0.0.1:6786",
		"127.0.0.1:6788",
	}

	fmt.Println("Creating client...")
	c, err := blazecache.New(servers...)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	c = c.WithStrategy(blazecache.ConsistentHashing)

	fmt.Println("Testing ping...")
	if err := c.Ping(); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("✓ Ping successful")

	fmt.Println("Testing PUT...")
	item := &blazecache.Item{
		Key:   "test_key_1",
		Value: []byte("test_value_1"),
	}
	if err := c.Set(item); err != nil {
		log.Fatalf("PUT failed: %v", err)
	}
	fmt.Println("✓ PUT successful")

	fmt.Println("Testing GET...")
	result, err := c.Get("test_key_1")
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	if result == nil {
		log.Fatalf("GET returned nil")
	}
	fmt.Printf("✓ GET successful: key=%s, value=%s\n", result.Key, string(result.Value))

	fmt.Println("\n✅ Single PUT test completed successfully!")
}
