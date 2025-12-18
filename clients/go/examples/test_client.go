package main

import (
	"fmt"
	"log"
	
	"github.com/blazecache/client"
)

func main() {
	fmt.Println("Testing BlazeCache Go client...")
	
	// Create client
	client, err := blazecache.New("127.0.0.1:6784")
	if err != nil {
		log.Fatal(err)
	}
	
	// Test ping
	if err := client.Ping(); err != nil {
		fmt.Printf("✗ Ping failed: %v\n", err)
	} else {
		fmt.Println("✓ Ping successful")
	}
	
	// Test set
	item := &blazecache.Item{
		Key:   "go-key",
		Value: []byte("go-value"),
	}
	
	if err := client.Set(item); err != nil {
		fmt.Printf("✗ Set failed: %v\n", err)
	} else {
		fmt.Println("✓ Set successful")
	}
	
	// Test get
	result, err := client.Get("go-key")
	if err != nil {
		if err == blazecache.ErrNotFound {
			fmt.Println("✗ Key not found")
		} else {
			fmt.Printf("✗ Get failed: %v\n", err)
		}
	} else {
		fmt.Printf("✓ Get successful: %s\n", string(result.Value))
	}
	
	// Test get non-existent key
	_, err = client.Get("nonexistent")
	if err == blazecache.ErrNotFound {
		fmt.Println("✓ Correctly returned not found for missing key")
	} else if err != nil {
		fmt.Printf("✗ Get failed: %v\n", err)
	} else {
		fmt.Println("✗ Should not have found key")
	}
	
	// Test delete
	if err := client.Delete("go-key"); err != nil {
		if err == blazecache.ErrNotFound {
			fmt.Println("✗ Key not found for delete")
		} else {
			fmt.Printf("✗ Delete failed: %v\n", err)
		}
	} else {
		fmt.Println("✓ Delete successful")
	}
	
	// Test multi-get
	client.Set(&blazecache.Item{Key: "key1", Value: []byte("value1")})
	client.Set(&blazecache.Item{Key: "key2", Value: []byte("value2")})
	
	results, err := client.GetMulti([]string{"key1", "key2", "key3"})
	if err != nil {
		fmt.Printf("✗ Multi-get failed: %v\n", err)
	} else {
		fmt.Printf("✓ Multi-get successful: %d keys found\n", len(results))
		for key, item := range results {
			fmt.Printf("  %s: %s\n", key, string(item.Value))
		}
	}
	
	fmt.Println("\n✅ Go client test completed!")
}
