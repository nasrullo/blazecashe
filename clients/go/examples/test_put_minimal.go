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
	fmt.Println("Testing minimal PUT...")
	
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6784", 5*time.Second)
	if err != nil {
		fmt.Printf("✗ Connection failed: %v\n", err)
		return
	}
	defer conn.Close()
	
	// Don't set deadline - let's see if it responds
	// conn.SetDeadline(time.Now().Add(10 * time.Second))
	
	// Encode PUT: [0x02][key_len:u16][key][data_len:u32][data][ttl:u32]
	key := "k"
	value := []byte("v")
	
	var buf bytes.Buffer
	buf.WriteByte(0x02) // PUT
	binary.Write(&buf, binary.BigEndian, uint16(len(key)))
	buf.WriteString(key)
	binary.Write(&buf, binary.BigEndian, uint32(len(value)))
	buf.Write(value)
	binary.Write(&buf, binary.BigEndian, uint32(0)) // TTL
	
	request := buf.Bytes()
	fmt.Printf("Sending %d bytes: %x\n", len(request), request)
	
	if _, err := conn.Write(request); err != nil {
		fmt.Printf("✗ Write failed: %v\n", err)
		return
	}
	
	fmt.Println("Waiting for response (5 second timeout)...")
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	
	statusBuf := make([]byte, 1)
	n, err := conn.Read(statusBuf)
	if err != nil {
		fmt.Printf("✗ Read failed (read %d bytes): %v\n", n, err)
		return
	}
	
	status := statusBuf[0]
	fmt.Printf("✓ Got status: 0x%02x\n", status)
	
	if status == 0x00 {
		var dataLen uint32
		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			fmt.Printf("✗ Read dataLen failed: %v\n", err)
			return
		}
		fmt.Printf("✓ PUT successful! Response data length: %d\n", dataLen)
	} else if status == 0x01 {
		var msgLen uint16
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			fmt.Printf("✗ Read msgLen failed: %v\n", err)
			return
		}
		msg := make([]byte, msgLen)
		io.ReadFull(conn, msg)
		fmt.Printf("✗ PUT failed: %s\n", string(msg))
	} else {
		fmt.Printf("✗ Unexpected status: 0x%02x\n", status)
	}
}
