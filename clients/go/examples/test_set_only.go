package main

import (
	"fmt"
	"log"
	"time"
	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := "127.0.0.1:6792"
	
	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	
	// Warm up
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("warmup-%d", i)
		value := []byte("warmup")
		item := &blazecache.Item{Key: key, Value: value}
		client.Set(item)
	}
	
	// Measure SET only
	fmt.Println("=== SET Operations Only ===")
	var times []time.Duration
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("set-test-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
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
	
	fmt.Printf("Average SET time: %s\n", avg)
	fmt.Printf("Throughput: %.2f ops/sec\n", 100.0/avg.Seconds())
}
