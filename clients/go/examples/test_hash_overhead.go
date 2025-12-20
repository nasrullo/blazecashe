package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"time"
	blazecache "github.com/blazecache/client"
)

func fnvHash64(input string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(input))
	return h.Sum64()
}

func main() {
	serverAddr := "127.0.0.1:6792"
	
	fmt.Println("=== Consistent Hashing Overhead Analysis ===")
	
	// Test hash calculation overhead
	fmt.Println("\n=== Hash Calculation Overhead ===")
	keys := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	
	start := time.Now()
	for _, key := range keys {
		_ = fnvHash64(key)
	}
	hashTime := time.Since(start)
	fmt.Printf("10,000 hash calculations: %s\n", hashTime)
	fmt.Printf("Per hash: %.2f ns\n", float64(hashTime.Nanoseconds())/10000)
	
	// Test binary search overhead
	fmt.Println("\n=== Binary Search Overhead ===")
	// Simulate hash ring with 150 replicas
	sortedHashes := make([]uint64, 150)
	for i := 0; i < 150; i++ {
		sortedHashes[i] = uint64(i * 1000000)
	}
	
	start = time.Now()
	for _, key := range keys {
		keyHash := fnvHash64(key)
		idx := sort.Search(len(sortedHashes), func(i int) bool {
			return sortedHashes[i] >= keyHash
		})
		if idx >= len(sortedHashes) {
			idx = 0
		}
		_ = idx
	}
	searchTime := time.Since(start)
	fmt.Printf("10,000 binary searches: %s\n", searchTime)
	fmt.Printf("Per search: %.2f ns\n", float64(searchTime.Nanoseconds())/10000)
	
	totalOverhead := hashTime + searchTime
	fmt.Printf("\nTotal overhead per operation: %.2f ns\n", float64(totalOverhead.Nanoseconds())/10000)
	fmt.Printf("Total overhead: %.2f µs\n", float64(totalOverhead.Nanoseconds())/10000/1000)
	
	// Compare with actual SET operation time
	fmt.Println("\n=== Actual SET Operation Time (Consistent Hashing) ===")
	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	client.WithStrategy(blazecache.ConsistentHashing)
	
	var times []time.Duration
	for i := 0; i < 100; i++ {
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
	for _, t := range times {
		sum += t
	}
	avg := sum / time.Duration(len(times))
	
	fmt.Printf("Average SET time: %s\n", avg)
	fmt.Printf("Hash+Search overhead: ~%.2f µs\n", float64(totalOverhead.Nanoseconds())/10000/1000)
	fmt.Printf("Overhead percentage: %.2f%%\n", (float64(totalOverhead.Nanoseconds())/10000/float64(avg.Nanoseconds()))*100)
}

