package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	servers := []string{
		"127.0.0.1:6784",
		"127.0.0.1:6786",
		"127.0.0.1:6788",
	}

	if len(os.Args) > 1 {
		servers = strings.Split(os.Args[1], ",")
	}

	ops := 100000
	if len(os.Args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil {
			ops = n
		}
	}

	c, err := blazecache.New(servers...)
	if err != nil {
		panic(err)
	}

	// Wait for servers to be ready
	for i := 0; i < 30; i++ {
		if err := c.Ping(); err == nil {
			break
		}
		if i == 29 {
			fmt.Println("❌ Servers not ready after 30 seconds")
			return
		}
		time.Sleep(1 * time.Second)
	}

	var ok, errs int64
	start := time.Now()

	for i := 0; i < ops; i++ {
		key := fmt.Sprintf("load-key-%d", i)
		val := []byte(fmt.Sprintf("value-%d", i))

		if err := c.Set(&blazecache.Item{Key: key, Value: val}); err != nil {
			fmt.Printf("SET error on %s: %v\n", key, err)
			errs++
			continue
		}

		item, err := c.Get(key)
		if err != nil {
			fmt.Printf("GET error on %s: %v\n", key, err)
			errs++
			continue
		}

		if item != nil && string(item.Value) == string(val) {
			ok++
		} else {
			fmt.Printf("Data mismatch on %s\n", key)
			errs++
		}
	}

	duration := time.Since(start)
	fmt.Printf("Load test complete. total_ops=%d success=%d errors=%d duration=%v\n", ops, ok, errs, duration)
}
