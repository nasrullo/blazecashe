package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	// Default servers from docker-compose
	servers := []string{
		"127.0.0.1:6784", // blazecache-1
		"127.0.0.1:6786", // blazecache-2
		"127.0.0.1:6788", // blazecache-3
	}

	if len(os.Args) > 1 {
		servers = strings.Split(os.Args[1], ",")
	}

	fmt.Println("🚀 Go Client Load Test")
	fmt.Println("======================")
	fmt.Printf("Servers: %v\n\n", servers)

	c, err := blazecache.New(servers...)
	if err != nil {
		panic(err)
	}

	// Wait for servers to be ready
	fmt.Println("Waiting for servers to be ready...")
	for i := 0; i < 30; i++ {
		err := c.Ping()
		if err == nil {
			break
		}
		if i == 29 {
			fmt.Println("❌ Servers not ready after 30 seconds")
			return
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Println("✓ Servers ready\n")

	// Test 1: Concurrent GET operations
	fmt.Println("📊 Test 1: Concurrent GET Operations")
	testConcurrentGet(c, 50, 100)

	// Test 2: Concurrent PUT operations
	fmt.Println("\n📊 Test 2: Concurrent PUT Operations")
	testConcurrentPut(c, 50, 200)

	// Test 3: Mixed workload
	fmt.Println("\n📊 Test 3: Mixed Workload (60% GET, 30% PUT, 10% DELETE)")
	testMixedWorkload(c, 1000)

	// Test 4: Sustained throughput
	fmt.Println("\n📊 Test 4: Sustained Throughput (10 seconds)")
	testSustainedThroughput(c, 10*time.Second)

	// Test 5: High connection count
	fmt.Println("\n📊 Test 5: High Connection Count")
	testHighConnectionCount(servers, 100, 50)

	fmt.Println("\n✅ Load test completed!")
}

func testConcurrentGet(c *blazecache.Client, numClients, opsPerClient int) {
	// Pre-populate cache
	for i := 0; i < 100; i++ {
		c.Set(&blazecache.Item{Key: fmt.Sprintf("key_%d", i), Value: []byte(fmt.Sprintf("value_%d", i))})
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalOps int64
	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			operations := 0
			for j := 0; j < opsPerClient; j++ {
				_, err := c.Get(fmt.Sprintf("key_%d", j%100))
				if err == nil {
					operations++
				}
			}
			mu.Lock()
			totalOps += int64(operations)
			mu.Unlock()
		}()
	}

	wg.Wait()
	duration := time.Since(start)
	opsPerSec := float64(totalOps) / duration.Seconds()

	fmt.Printf("  Operations: %d\n", totalOps)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  Throughput: %.2f ops/sec\n", opsPerSec)
	fmt.Printf("  Latency (avg): %.2f ms\n", duration.Seconds()*1000.0/float64(totalOps))
}

func testConcurrentPut(c *blazecache.Client, numClients, opsPerClient int) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalOps int64
	start := time.Now()

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			operations := 0
			for i := 0; i < opsPerClient; i++ {
				key := fmt.Sprintf("put_key_%d_%d", id, i)
				err := c.Set(&blazecache.Item{Key: key, Value: []byte("value")})
				if err == nil {
					operations++
				}
			}
			mu.Lock()
			totalOps += int64(operations)
			mu.Unlock()
		}(clientID)
	}

	wg.Wait()
	duration := time.Since(start)
	opsPerSec := float64(totalOps) / duration.Seconds()

	fmt.Printf("  Operations: %d\n", totalOps)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  Throughput: %.2f ops/sec\n", opsPerSec)
	fmt.Printf("  Latency (avg): %.2f ms\n", duration.Seconds()*1000.0/float64(totalOps))
}

func testMixedWorkload(c *blazecache.Client, totalOps int) {
	// Pre-populate some keys
	for i := 0; i < 100; i++ {
		c.Set(&blazecache.Item{Key: fmt.Sprintf("mixed_key_%d", i), Value: []byte("value")})
	}

	start := time.Now()
	var getOps, putOps, deleteOps int64

	for i := 0; i < totalOps; i++ {
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5:
			// 60% GET
			_, err := c.Get(fmt.Sprintf("mixed_key_%d", i%100))
			if err == nil {
				getOps++
			}
		case 6, 7, 8:
			// 30% PUT
			err := c.Set(&blazecache.Item{Key: fmt.Sprintf("mixed_key_%d", i), Value: []byte("new_value")})
			if err == nil {
				putOps++
			}
		case 9:
			// 10% DELETE
			err := c.Delete(fmt.Sprintf("mixed_key_%d", i%100))
			if err == nil {
				deleteOps++
			}
		}
	}

	duration := time.Since(start)
	totalOpsDone := getOps + putOps + deleteOps
	opsPerSec := float64(totalOpsDone) / duration.Seconds()

	fmt.Printf("  GET operations: %d\n", getOps)
	fmt.Printf("  PUT operations: %d\n", putOps)
	fmt.Printf("  DELETE operations: %d\n", deleteOps)
	fmt.Printf("  Total operations: %d\n", totalOpsDone)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  Throughput: %.2f ops/sec\n", opsPerSec)
}

func testSustainedThroughput(c *blazecache.Client, duration time.Duration) {
	start := time.Now()
	var operations int64

	for time.Since(start) < duration {
		key := fmt.Sprintf("sustained_key_%d", operations)
		err := c.Set(&blazecache.Item{Key: key, Value: []byte("value")})
		if err == nil {
			operations++
		}
	}

	actualDuration := time.Since(start)
	opsPerSec := float64(operations) / actualDuration.Seconds()

	fmt.Printf("  Operations: %d\n", operations)
	fmt.Printf("  Duration: %v\n", actualDuration)
	fmt.Printf("  Throughput: %.2f ops/sec\n", opsPerSec)
}

func testHighConnectionCount(servers []string, numClients, opsPerClient int) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalOps int64
	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := blazecache.New(servers...)
			if err != nil {
				return
			}
			operations := 0
			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("conn_key_%d", j)
				err := c.Set(&blazecache.Item{Key: key, Value: []byte("value")})
				if err == nil {
					operations++
				}
			}
			mu.Lock()
			totalOps += int64(operations)
			mu.Unlock()
		}()
	}

	wg.Wait()
	duration := time.Since(start)
	opsPerSec := float64(totalOps) / duration.Seconds()

	fmt.Printf("  Clients: %d\n", numClients)
	fmt.Printf("  Operations: %d\n", totalOps)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  Throughput: %.2f ops/sec\n", opsPerSec)
}
