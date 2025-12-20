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
	
	fmt.Println("=== SET Operation Timing (1000 operations) ===")
	var times []time.Duration
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		item := &blazecache.Item{Key: key, Value: value}
		
		start := time.Now()
		if err := client.Set(item); err != nil {
			log.Fatalf("Set failed: %v", err)
		}
		times = append(times, time.Since(start))
	}
	
	var sum time.Duration
	var min, max time.Duration = times[0], times[0]
	for _, t := range times {
		sum += t
		if t < min {
			min = t
		}
		if t > max {
			max = t
		}
	}
	avg := sum / time.Duration(len(times))
	
	fmt.Printf("Min: %s\n", min)
	fmt.Printf("Max: %s\n", max)
	fmt.Printf("Avg: %s\n", avg)
	fmt.Printf("Total: %s\n", sum)
	fmt.Printf("Throughput: %.2f ops/sec\n", 1000.0/sum.Seconds())
	
	// Check if there's a pattern
	fmt.Println("\n=== First 10 vs Last 10 ===")
	var firstSum, lastSum time.Duration
	for i := 0; i < 10; i++ {
		firstSum += times[i]
		lastSum += times[len(times)-10+i]
	}
	fmt.Printf("First 10 avg: %s\n", firstSum/10)
	fmt.Printf("Last 10 avg: %s\n", lastSum/10)
}
