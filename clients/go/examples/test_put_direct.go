package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

func main() {
	fmt.Println("Testing direct connection (bypassing pool)...")
	
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6784", 5*time.Second)
	if err != nil {
		fmt.Printf("✗ Connection failed: %v\n", err)
		return
	}
	defer conn.Close()
	
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	
	// Encode PUT command
	key := "test-key"
	value := []byte("test-value")
	var buf bytes.Buffer
	buf.WriteByte(0x02) // PUT
	binary.Write(&buf, binary.BigEndian, uint16(len(key)))
	buf.WriteString(key)
	binary.Write(&buf, binary.BigEndian, uint32(len(value)))
	buf.Write(value)
	binary.Write(&buf, binary.BigEndian, uint32(0)) // TTL
	
	fmt.Println("Sending PUT request...")
	if _, err := conn.Write(buf.Bytes()); err != nil {
		fmt.Printf("✗ Write failed: %v\n", err)
		return
	}
	
	fmt.Println("Reading response...")
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		fmt.Printf("✗ Read status failed: %v\n", err)
		return
	}
	
	status := statusBuf[0]
	fmt.Printf("✓ Got status: %d\n", status)
	
	if status == 0x00 {
		var dataLen uint32
		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			fmt.Printf("✗ Read dataLen failed: %v\n", err)
			return
		}
		fmt.Printf("✓ PUT successful! Data length: %d\n", dataLen)
	} else {
		fmt.Printf("✗ PUT failed with status: %d\n", status)
	}
}
