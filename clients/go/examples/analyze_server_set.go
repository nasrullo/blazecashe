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
	
	fmt.Println("=== Analyzing SET Operation Breakdown ===")
	fmt.Println("Measuring individual SET operations...\n")
	
	// Measure SET operations with different value sizes
	valueSizes := []int{10, 100, 1000, 10000}
	
	for _, size := range valueSizes {
		value := make([]byte, size)
		for i := range value {
			value[i] = byte(i % 256)
		}
		
		var times []time.Duration
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test-size-%d-%d", size, i)
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
		
		fmt.Printf("Value size %5d bytes: avg %s, throughput %.2f ops/sec\n", 
			size, avg, 100.0/sum.Seconds())
	}
	
	fmt.Println("\n=== Comparing SET vs GET ===")
	// SET
	key := "compare-test"
	value := []byte("test-value")
	item := &blazecache.Item{Key: key, Value: value}
	
	start1 := time.Now()
	for i := 0; i < 1000; i++ {
		client.Set(item)
	}
	setTime := time.Since(start1)
	
	// GET
	start2 := time.Now()
	for i := 0; i < 1000; i++ {
		client.Get(key)
	}
	getTime := time.Since(start2)
	
	fmt.Printf("1000 SET operations: %s (%.2f ops/sec)\n", setTime, 1000.0/setTime.Seconds())
	fmt.Printf("1000 GET operations: %s (%.2f ops/sec)\n", getTime, 1000.0/getTime.Seconds())
	fmt.Printf("SET is %.2fx slower than GET\n", setTime.Seconds()/getTime.Seconds())
}
