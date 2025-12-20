package main

import (
	"fmt"
	"io"
	"net"
	"time"
)

func main() {
	fmt.Println("Testing PING with direct connection...")
	
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6784", 5*time.Second)
	if err != nil {
		fmt.Printf("✗ Connection failed: %v\n", err)
		return
	}
	defer conn.Close()
	
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	
	fmt.Println("Sending PING...")
	if _, err := conn.Write([]byte{0x00}); err != nil {
		fmt.Printf("✗ Write failed: %v\n", err)
		return
	}
	
	fmt.Println("Reading response...")
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		fmt.Printf("✗ Read failed: %v\n", err)
		return
	}
	
	status := statusBuf[0]
	if status == 0x02 {
		fmt.Println("✓ PING successful! Got PONG")
	} else {
		fmt.Printf("✗ Unexpected status: %d\n", status)
	}
}
