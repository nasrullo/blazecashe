package main

import (
	"fmt"
	"log"
	"sync"
	"time"
	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := "127.0.0.1:6792"
	
	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	
	// Test with fewer workers first
	fmt.Println("=== Testing with 10 workers, 1000 ops each ===")
	var wg sync.WaitGroup
	start := time.Now()
	
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("test-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d", i))
				item := &blazecache.Item{Key: key, Value: value}
				if err := client.Set(item); err != nil {
					log.Printf("Set failed: %v", err)
					return
				}
			}
		}(w)
	}
	
	wg.Wait()
	elapsed := time.Since(start)
	
	fmt.Printf("Time: %s\n", elapsed)
	fmt.Printf("Throughput: %.2f ops/sec\n", 10000.0/elapsed.Seconds())
}
