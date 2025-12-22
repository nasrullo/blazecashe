package blazecache

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestUDPClientPing(t *testing.T) {
	// This test requires a running server
	// Skip if server is not available
	client, err := NewUDPClient("127.0.0.1:6793")
	if err != nil {
		t.Skipf("Failed to create UDP client: %v", err)
	}
	defer client.Close()

	// Set a short timeout for testing
	client.conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	err = client.Ping()
	if err != nil {
		t.Logf("Ping failed (server may not be running): %v", err)
	}
}

func TestUDPClientEncodeDecode(t *testing.T) {
	// Test single datagram encoding
	requestID := uint32(12345)
	command := byte(0x01)
	data := []byte("test data")

	packet := encodeSingleDatagram(requestID, command, data)

	// Verify magic number
	magic := uint16(packet[0])<<8 | uint16(packet[1])
	if magic != UDP_MAGIC {
		t.Errorf("Expected magic %x, got %x", UDP_MAGIC, magic)
	}

	// Verify version
	if packet[2] != UDP_VERSION {
		t.Errorf("Expected version %d, got %d", UDP_VERSION, packet[2])
	}

	// Verify request ID
	recvRequestID := binary.BigEndian.Uint32(packet[4:8])
	if recvRequestID != requestID {
		t.Errorf("Expected request ID %d, got %d", requestID, recvRequestID)
	}

	// Verify command
	if packet[8] != command {
		t.Errorf("Expected command %d, got %d", command, packet[8])
	}

	// Verify data
	if !bytes.Equal(packet[9:], data) {
		t.Errorf("Expected data %v, got %v", data, packet[9:])
	}

	// Test decoding
	decodedRequestID, decodedCommand, decodedData, err := decodeSingleDatagram(packet)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	// Note: decodeSingleDatagram expects a response (FLAG_RESPONSE), so we need to modify the packet
	responsePacket := make([]byte, len(packet))
	copy(responsePacket, packet)
	responsePacket[3] = UDP_FLAG_RESPONSE // Set response flag

	decodedRequestID, decodedCommand, decodedData, err = decodeSingleDatagram(responsePacket)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if decodedRequestID != requestID {
		t.Errorf("Decoded request ID mismatch: expected %d, got %d", requestID, decodedRequestID)
	}
	if decodedCommand != command {
		t.Errorf("Decoded command mismatch: expected %d, got %d", command, decodedCommand)
	}
	if !bytes.Equal(decodedData, data) {
		t.Errorf("Decoded data mismatch: expected %v, got %v", data, decodedData)
	}
}

func TestUDPClientFragmentation(t *testing.T) {
	requestID := uint32(54321)
	msgType := byte(UDP_FLAG_REQUEST)

	// Create a message larger than MAX_PAYLOAD
	largeData := make([]byte, UDP_MAX_PAYLOAD*3)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	fragments, err := fragmentMessage(requestID, msgType, largeData)
	if err != nil {
		t.Fatalf("Failed to fragment message: %v", err)
	}

	if len(fragments) != 3 {
		t.Errorf("Expected 3 fragments, got %d", len(fragments))
	}

	// Verify each fragment
	for i, fragment := range fragments {
		fragMsgType, fragRequestID, seqNo, fragCount, payloadLen, err := decodeFragmentHeader(fragment)
		if err != nil {
			t.Fatalf("Failed to decode fragment %d: %v", i, err)
		}

		if fragMsgType != msgType {
			t.Errorf("Fragment %d: expected msg type %d, got %d", i, msgType, fragMsgType)
		}
		if fragRequestID != requestID {
			t.Errorf("Fragment %d: expected request ID %d, got %d", i, requestID, fragRequestID)
		}
		if seqNo != uint16(i) {
			t.Errorf("Fragment %d: expected seq %d, got %d", i, i, seqNo)
		}
		if fragCount != 3 {
			t.Errorf("Fragment %d: expected frag count 3, got %d", i, fragCount)
		}

		expectedPayloadLen := UDP_MAX_PAYLOAD
		if i == 2 {
			// Last fragment may be smaller
			expectedPayloadLen = len(largeData) - (2 * UDP_MAX_PAYLOAD)
		}
		if payloadLen != uint16(expectedPayloadLen) {
			t.Errorf("Fragment %d: expected payload len %d, got %d", i, expectedPayloadLen, payloadLen)
		}
	}
}

func TestReassemblyState(t *testing.T) {
	fragCount := uint16(3)
	reassembly := newReassemblyState(fragCount)

	// Insert fragments out of order
	reassembly.insert(2, []byte("fragment 2"))
	reassembly.insert(0, []byte("fragment 0"))
	reassembly.insert(1, []byte("fragment 1"))

	if !reassembly.isComplete() {
		t.Error("Reassembly should be complete")
	}

	assembled := reassembly.assemble()
	expected := []byte("fragment 0fragment 1fragment 2")
	if !bytes.Equal(assembled, expected) {
		t.Errorf("Expected %v, got %v", expected, assembled)
	}
}

