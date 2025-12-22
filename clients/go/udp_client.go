package blazecache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// UDP protocol constants (matching Rust implementation)
const (
	UDP_MAGIC              = 0xBC01
	UDP_VERSION            = 1
	UDP_MAX_DATAGRAM       = 1200                  // QUIC-safe MTU
	UDP_MAX_PAYLOAD        = UDP_MAX_DATAGRAM - 14 // Fragment header size
	UDP_MAX_MESSAGE_BYTES  = 4 * 1024 * 1024       // 4MB max message
	UDP_HEADER_LEN         = 14                    // Fragment header length
	UDP_SINGLE_HEADER_LEN  = 9                     // Single datagram header length
	UDP_REASSEMBLY_TIMEOUT = 2 * time.Second
	UDP_CLIENT_TIMEOUT     = 5 * time.Second
)

// Message type flags
const (
	UDP_FLAG_REQUEST  = 0
	UDP_FLAG_RESPONSE = 1
)

// UDPClient implements a UDP-based client with QUIC-like features
// - Fragmentation and reassembly for large messages
// - Request ID-based multiplexing
// - Fast path for small single-datagram messages
// - Automatic retry logic
type UDPClient struct {
	conn       *net.UDPConn
	serverAddr *net.UDPAddr
	requestID  uint32
	mu         sync.Mutex
}

// NewUDPClient creates a new UDP client connected to the specified server
func NewUDPClient(serverAddr string) (*UDPClient, error) {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid server address: %w", err)
	}

	// Create UDP socket with optimized settings
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP socket: %w", err)
	}

	// Set socket buffer sizes for high throughput (QUIC-like optimization)
	conn.SetReadBuffer(4 * 1024 * 1024)  // 4MB receive buffer
	conn.SetWriteBuffer(4 * 1024 * 1024) // 4MB send buffer

	return &UDPClient{
		conn:       conn,
		serverAddr: addr,
		requestID:  0,
	}, nil
}

// Close closes the UDP connection
func (c *UDPClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// nextRequestID atomically increments and returns the next request ID
func (c *UDPClient) nextRequestID() uint32 {
	return atomic.AddUint32(&c.requestID, 1)
}

// encodeSingleDatagram encodes a small message that fits in a single datagram (QUIC fast path)
// Format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][CMD:1][DATA:...]
func encodeSingleDatagram(requestID uint32, command byte, data []byte) []byte {
	packetSize := UDP_SINGLE_HEADER_LEN + len(data)
	packet := make([]byte, 0, packetSize)

	// Magic number
	packet = append(packet, byte(UDP_MAGIC>>8), byte(UDP_MAGIC&0xFF))
	// Version
	packet = append(packet, UDP_VERSION)
	// Flags (0 = Request)
	packet = append(packet, UDP_FLAG_REQUEST)
	// Request ID
	requestIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(requestIDBytes, requestID)
	packet = append(packet, requestIDBytes...)
	// Command
	packet = append(packet, command)
	// Data
	packet = append(packet, data...)

	return packet
}

// encodeFragment encodes a fragment header and payload
// Format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][SEQ:2][FRAG_COUNT:2][PAYLOAD_LEN:2][DATA:...]
func encodeFragment(msgType byte, requestID uint32, seqNo uint16, fragCount uint16, payload []byte) []byte {
	packet := make([]byte, 0, UDP_HEADER_LEN+len(payload))

	// Magic number
	packet = append(packet, byte(UDP_MAGIC>>8), byte(UDP_MAGIC&0xFF))
	// Version
	packet = append(packet, UDP_VERSION)
	// Flags
	packet = append(packet, msgType)
	// Request ID
	requestIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(requestIDBytes, requestID)
	packet = append(packet, requestIDBytes...)
	// Sequence number
	seqBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(seqBytes, seqNo)
	packet = append(packet, seqBytes...)
	// Fragment count
	fragCountBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(fragCountBytes, fragCount)
	packet = append(packet, fragCountBytes...)
	// Payload length
	payloadLenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(payloadLenBytes, uint16(len(payload)))
	packet = append(packet, payloadLenBytes...)
	// Payload
	packet = append(packet, payload...)

	return packet
}

// fragmentMessage splits a large message into fragments (QUIC datagram splitting)
func fragmentMessage(requestID uint32, msgType byte, data []byte) ([][]byte, error) {
	if len(data) > UDP_MAX_MESSAGE_BYTES {
		return nil, errors.New("message too large")
	}

	fragCount := (len(data) + UDP_MAX_PAYLOAD - 1) / UDP_MAX_PAYLOAD
	if fragCount == 0 {
		fragCount = 1
	}
	if fragCount > 65535 {
		return nil, errors.New("too many fragments")
	}

	fragments := make([][]byte, 0, fragCount)
	for i := 0; i < fragCount; i++ {
		start := i * UDP_MAX_PAYLOAD
		end := start + UDP_MAX_PAYLOAD
		if end > len(data) {
			end = len(data)
		}
		payload := data[start:end]
		fragment := encodeFragment(msgType, requestID, uint16(i), uint16(fragCount), payload)
		fragments = append(fragments, fragment)
	}

	return fragments, nil
}

// decodeFragmentHeader decodes a fragment header from a UDP datagram
func decodeFragmentHeader(buf []byte) (msgType byte, requestID uint32, seqNo uint16, fragCount uint16, payloadLen uint16, err error) {
	if len(buf) < UDP_HEADER_LEN {
		return 0, 0, 0, 0, 0, errors.New("datagram too short")
	}

	// Check magic number
	magic := binary.BigEndian.Uint16(buf[0:2])
	if magic != UDP_MAGIC {
		return 0, 0, 0, 0, 0, errors.New("bad magic number")
	}

	// Check version
	version := buf[2]
	if version != UDP_VERSION {
		return 0, 0, 0, 0, 0, errors.New("unsupported version")
	}

	msgType = buf[3]
	requestID = binary.BigEndian.Uint32(buf[4:8])
	seqNo = binary.BigEndian.Uint16(buf[8:10])
	fragCount = binary.BigEndian.Uint16(buf[10:12])
	payloadLen = binary.BigEndian.Uint16(buf[12:14])

	if fragCount == 0 {
		return 0, 0, 0, 0, 0, errors.New("frag_count=0")
	}
	if seqNo >= fragCount {
		return 0, 0, 0, 0, 0, errors.New("seq_no out of range")
	}
	if payloadLen > UDP_MAX_PAYLOAD {
		return 0, 0, 0, 0, 0, errors.New("payload too large")
	}

	return msgType, requestID, seqNo, fragCount, payloadLen, nil
}

// decodeSingleDatagram decodes a single-datagram message (QUIC fast path)
// Format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][STATUS:1][DATA:...]
func decodeSingleDatagram(buf []byte) (requestID uint32, status byte, data []byte, err error) {
	if len(buf) < UDP_SINGLE_HEADER_LEN {
		return 0, 0, nil, errors.New("datagram too short")
	}

	// Check magic number
	magic := binary.BigEndian.Uint16(buf[0:2])
	if magic != UDP_MAGIC {
		return 0, 0, nil, errors.New("bad magic number")
	}

	// Check version
	version := buf[2]
	if version != UDP_VERSION {
		return 0, 0, nil, errors.New("unsupported version")
	}

	flags := buf[3]
	if flags != UDP_FLAG_RESPONSE {
		return 0, 0, nil, errors.New("not a response")
	}

	requestID = binary.BigEndian.Uint32(buf[4:8])
	status = buf[8] // Status byte (0x00=OK, 0x01=NOT_FOUND, 0x02=ERROR)
	data = buf[9:]  // Data starts after status byte

	return requestID, status, data, nil
}

// ReassemblyState tracks fragments for reassembly (QUIC datagram reassembly)
type ReassemblyState struct {
	fragCount uint16
	received  map[uint16][]byte
	createdAt time.Time
}

// newReassemblyState creates a new reassembly state
func newReassemblyState(fragCount uint16) *ReassemblyState {
	return &ReassemblyState{
		fragCount: fragCount,
		received:  make(map[uint16][]byte),
		createdAt: time.Now(),
	}
}

// insert adds a fragment to the reassembly state
func (r *ReassemblyState) insert(seqNo uint16, payload []byte) error {
	if seqNo >= r.fragCount {
		return errors.New("seq_no out of range")
	}
	r.received[seqNo] = payload
	return nil
}

// isComplete checks if all fragments have been received
func (r *ReassemblyState) isComplete() bool {
	return len(r.received) == int(r.fragCount)
}

// assemble combines all fragments into the complete message
func (r *ReassemblyState) assemble() []byte {
	if !r.isComplete() {
		return nil
	}

	totalLen := 0
	for i := uint16(0); i < r.fragCount; i++ {
		totalLen += len(r.received[i])
	}

	result := make([]byte, 0, totalLen)
	for i := uint16(0); i < r.fragCount; i++ {
		result = append(result, r.received[i]...)
	}

	return result
}

// Ping sends a PING request and waits for a PONG response
func (c *UDPClient) Ping() error {
	requestID := c.nextRequestID()
	packet := encodeSingleDatagram(requestID, 0x04, nil) // 0x04 = PING command

	// Send packet
	if _, err := c.conn.WriteToUDP(packet, c.serverAddr); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}

	// Receive response with timeout
	c.conn.SetReadDeadline(time.Now().Add(UDP_CLIENT_TIMEOUT))
	defer c.conn.SetReadDeadline(time.Time{})

	buffer := make([]byte, UDP_MAX_DATAGRAM)
	for {
		n, _, err := c.conn.ReadFromUDP(buffer)
		if err != nil {
			return fmt.Errorf("failed to receive pong: %w", err)
		}

		// Try to decode as single datagram first
		recvRequestID, command, _, err := decodeSingleDatagram(buffer[:n])
		if err == nil && recvRequestID == requestID {
			if command == 0x00 { // PONG response
				return nil
			}
			return errors.New("ping failed")
		}

		// Check if it's a fragment (would need reassembly, but PING should be single datagram)
		// For now, just continue waiting
	}
}

// Get retrieves a value from the cache
func (c *UDPClient) Get(key string) ([]byte, error) {
	requestID := c.nextRequestID()
	keyBytes := []byte(key)
	keyLen := uint16(len(keyBytes))

	// Encode GET command data: [KEY_LEN:2][KEY:bytes]
	// The command byte (0x01) is encoded in the single datagram header
	var cmdData bytes.Buffer
	keyLenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(keyLenBytes, keyLen)
	cmdData.Write(keyLenBytes)
	cmdData.Write(keyBytes)

	// Check if it fits in a single datagram (QUIC fast path)
	packetSize := UDP_SINGLE_HEADER_LEN + cmdData.Len()
	if packetSize <= UDP_MAX_DATAGRAM {
		// Single datagram - use fast path
		packet := encodeSingleDatagram(requestID, 0x01, cmdData.Bytes())

		if _, err := c.conn.WriteToUDP(packet, c.serverAddr); err != nil {
			return nil, fmt.Errorf("failed to send get request: %w", err)
		}

		return c.receiveResponse(requestID)
	}

	// Large message - fragment it
	fragments, err := fragmentMessage(requestID, UDP_FLAG_REQUEST, cmdData.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to fragment message: %w", err)
	}

	// Send all fragments
	for _, fragment := range fragments {
		if _, err := c.conn.WriteToUDP(fragment, c.serverAddr); err != nil {
			return nil, fmt.Errorf("failed to send fragment: %w", err)
		}
		// Small delay between fragments to avoid overwhelming the network
		time.Sleep(1 * time.Millisecond)
	}

	return c.receiveResponse(requestID)
}

// Set stores a value in the cache
func (c *UDPClient) Set(key string, value []byte, ttl uint32) error {
	requestID := c.nextRequestID()
	keyBytes := []byte(key)
	keyLen := uint16(len(keyBytes))
	valueLen := uint32(len(value))

	// Encode PUT command data: [KEY_LEN:2][KEY:bytes][VALUE_LEN:4][VALUE:bytes][TTL:4]
	// The command byte (0x02) is encoded in the single datagram header
	var cmdData bytes.Buffer
	keyLenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(keyLenBytes, keyLen)
	cmdData.Write(keyLenBytes)
	cmdData.Write(keyBytes)
	valueLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(valueLenBytes, valueLen)
	cmdData.Write(valueLenBytes)
	cmdData.Write(value)
	ttlBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(ttlBytes, ttl)
	cmdData.Write(ttlBytes)

	cmdBytes := cmdData.Bytes()

	// Check if it fits in a single datagram (QUIC fast path)
	packetSize := UDP_SINGLE_HEADER_LEN + len(cmdBytes)
	if packetSize <= UDP_MAX_DATAGRAM {
		// Single datagram - use fast path
		packet := encodeSingleDatagram(requestID, 0x02, cmdBytes)

		if _, err := c.conn.WriteToUDP(packet, c.serverAddr); err != nil {
			return fmt.Errorf("failed to send set request: %w", err)
		}

		_, err := c.receiveResponse(requestID)
		return err
	}

	// Large message - fragment it
	fragments, err := fragmentMessage(requestID, UDP_FLAG_REQUEST, cmdBytes)
	if err != nil {
		return fmt.Errorf("failed to fragment message: %w", err)
	}

	// Send all fragments
	for _, fragment := range fragments {
		if _, err := c.conn.WriteToUDP(fragment, c.serverAddr); err != nil {
			return fmt.Errorf("failed to send fragment: %w", err)
		}
		// Small delay between fragments to avoid overwhelming the network
		time.Sleep(1 * time.Millisecond)
	}

	_, err = c.receiveResponse(requestID)
	return err
}

// receiveResponse receives and reassembles a response (QUIC datagram reassembly)
func (c *UDPClient) receiveResponse(requestID uint32) ([]byte, error) {
	deadline := time.Now().Add(UDP_CLIENT_TIMEOUT)
	var reassembly *ReassemblyState
	buffer := make([]byte, UDP_MAX_DATAGRAM)
	attempts := 0

	// Set initial read deadline
	c.conn.SetReadDeadline(deadline)
	defer c.conn.SetReadDeadline(time.Time{}) // Clear deadline when done

	for {
		// Check deadline before reading
		now := time.Now()
		if now.After(deadline) {
			return nil, fmt.Errorf("response timeout after %d attempts (request_id=%d)", attempts, requestID)
		}

		// Update read deadline with remaining time
		remaining := deadline.Sub(now)
		if remaining <= 0 {
			return nil, fmt.Errorf("response timeout after %d attempts (request_id=%d)", attempts, requestID)
		}
		c.conn.SetReadDeadline(now.Add(remaining))

		n, _, err := c.conn.ReadFromUDP(buffer)
		if err != nil {
			// Check if it's a timeout
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Timeout - check if we still have time overall
				if time.Now().After(deadline) {
					return nil, fmt.Errorf("response timeout after %d attempts (request_id=%d)", attempts, requestID)
				}
				continue // Try again if we have time
			}
			return nil, fmt.Errorf("failed to receive response: %w", err)
		}

		attempts++

		// Validate basic packet format first (like Rust client)
		if n < 9 {
			continue // Too short, skip
		}

		// Check magic number
		magic := binary.BigEndian.Uint16(buffer[0:2])
		if magic != UDP_MAGIC {
			continue // Invalid magic, skip
		}

		// Check version
		version := buffer[2]
		if version != UDP_VERSION {
			continue // Invalid version, skip
		}

		// Check if it's a response
		flags := buffer[3]
		if flags != UDP_FLAG_RESPONSE {
			continue // Not a response, skip
		}

		// Extract request ID
		recvRequestID := binary.BigEndian.Uint32(buffer[4:8])

		// Check if this is our response
		if recvRequestID != requestID {
			continue // Wrong request ID, continue waiting
		}

		// Got the right response! Now parse it
		// Check if it's a single datagram (byte 8 is status) or fragment (bytes 8-9 are seq_no)
		byte8 := buffer[8]

		// Single datagram format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][STATUS:1][DATA:...]
		// Fragment format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][SEQ:2][FRAG_COUNT:2][PAYLOAD_LEN:2][DATA:...]
		// For single datagram, byte 8 is the status (0x00-0x02)
		// For fragments, bytes 8-9 are seq_no (u16), which could be 0-65535
		// If byte 8 is <= 0x02, it's likely a status byte (single datagram)
		if byte8 <= 0x02 && n >= 9 {
			// Single datagram response
			status := byte8
			data := buffer[9:n]
			c.conn.SetReadDeadline(time.Time{}) // Clear deadline
			return c.parseResponse(status, data)
		}

		// Try to decode as fragment
		if n < UDP_HEADER_LEN {
			continue // Too short for fragment header
		}

		seqNo := binary.BigEndian.Uint16(buffer[8:10])
		fragCount := binary.BigEndian.Uint16(buffer[10:12])
		payloadLen := binary.BigEndian.Uint16(buffer[12:14])

		if fragCount == 0 || seqNo >= fragCount {
			continue // Invalid fragment
		}
		if payloadLen > UDP_MAX_PAYLOAD {
			continue // Payload too large
		}
		if n < UDP_HEADER_LEN+int(payloadLen) {
			continue // Incomplete fragment
		}
		payload := buffer[UDP_HEADER_LEN : UDP_HEADER_LEN+int(payloadLen)]

		// Initialize or get reassembly state
		if reassembly == nil {
			reassembly = newReassemblyState(fragCount)
		} else if reassembly.fragCount != fragCount {
			// Fragment count changed, reset
			reassembly = newReassemblyState(fragCount)
		}

		// Insert fragment
		if err := reassembly.insert(seqNo, payload); err != nil {
			continue
		}

		// Check if complete
		if reassembly.isComplete() {
			completeData := reassembly.assemble()
			// Parse the complete response
			// The first byte should be the command/status
			if len(completeData) < 1 {
				return nil, errors.New("invalid response")
			}
			status := completeData[0]
			c.conn.SetReadDeadline(time.Time{}) // Clear deadline
			return c.parseResponse(status, completeData[1:])
		}
	}
}

// parseResponse parses a response and returns the data or error
// Status byte meanings:
//
//	0x00 = OK (with data for GET, no data for PUT)
//	0x01 = NOT FOUND (no additional data)
//	0x02 = ERROR (with error message)
func (c *UDPClient) parseResponse(status byte, data []byte) ([]byte, error) {
	switch status {
	case 0x00: // OK
		// For GET: Format is [VALUE_LEN:4][VALUE:bytes]
		// For PUT: No data (empty response)
		if len(data) == 0 {
			return nil, nil // Empty response (PUT success)
		}
		// GET response with data
		if len(data) < 4 {
			return nil, errors.New("invalid response format")
		}
		dataLen := binary.BigEndian.Uint32(data[0:4])
		if dataLen == 0 {
			return nil, nil // Empty value
		}
		if len(data) < 4+int(dataLen) {
			return nil, errors.New("incomplete response data")
		}
		return data[4 : 4+dataLen], nil
	case 0x01: // NOT FOUND
		// No additional data, just the status byte
		return nil, errors.New("key not found")
	case 0x02: // ERROR
		// Format: [MSG_LEN:2][MSG:bytes]
		if len(data) < 2 {
			return nil, errors.New("invalid error response format")
		}
		msgLen := binary.BigEndian.Uint16(data[0:2])
		if len(data) < 2+int(msgLen) {
			return nil, errors.New("incomplete error message")
		}
		msg := string(data[2 : 2+msgLen])
		return nil, errors.New("server error: " + msg)
	default:
		return nil, fmt.Errorf("unknown response status: %d", status)
	}
}
