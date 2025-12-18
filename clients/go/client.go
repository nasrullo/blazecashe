package blazecache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound = errors.New("key not found")
	ErrTimeout  = errors.New("operation timeout")
)

type SelectionStrategy int

const (
	RoundRobin SelectionStrategy = iota
	ConsistentHashing
)

type Client struct {
	servers     []string
	strategy    SelectionStrategy
	counter     uint64
	timeout     time.Duration
	hashRing    *consistentHashRing
	mu          sync.RWMutex
	seed        string        // for discovery mode
	refreshSecs int           // for discovery mode
	stopRefresh chan struct{} // to stop the refresh goroutine
}

type consistentHashRing struct {
	sortedHashes  []uint64 // sorted hash values for binary search (better cache locality)
	serverIndices []int    // parallel array: index into servers array for each hash
	servers       []string // server addresses (no duplication)
	replicas      int
}

type Item struct {
	Key   string
	Value []byte
}

func New(servers ...string) (*Client, error) {
	if len(servers) == 0 {
		return nil, errors.New("at least one server required")
	}

	c := &Client{
		servers:  servers,
		strategy: RoundRobin,
		timeout:  5 * time.Second,
	}
	c.rebuildHashRing()
	return c, nil
}

func (c *Client) WithStrategy(strategy SelectionStrategy, weights ...uint32) *Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.strategy = strategy
	c.rebuildHashRing()
	return c
}

func (c *Client) WithTimeout(timeout time.Duration) *Client {
	c.timeout = timeout
	return c
}

// WithDiscovery creates a client that discovers peers via the PEER command from a seed node and refreshes periodically.
// The client will use consistent hashing and automatically update the server list and hash ring.
func WithDiscovery(seed string, refreshSecs int) (*Client, error) {
	if refreshSecs < 1 {
		refreshSecs = 1
	}

	c := &Client{
		servers:     []string{seed},
		strategy:    ConsistentHashing,
		timeout:     5 * time.Second,
		seed:        seed,
		refreshSecs: refreshSecs,
		stopRefresh: make(chan struct{}),
	}
	c.rebuildHashRing()

	// Start background goroutine to refresh peers
	go c.refreshPeersLoop()

	return c, nil
}

func (c *Client) refreshPeersLoop() {
	ticker := time.NewTicker(time.Duration(c.refreshSecs) * time.Second)
	defer ticker.Stop()

	// Do initial refresh immediately
	if err := c.refreshPeers(); err != nil {
		log.Printf("peer refresh failed: %v", err)
	}

	for {
		select {
		case <-ticker.C:
			if err := c.refreshPeers(); err != nil {
				log.Printf("peer refresh failed: %v", err)
			}
		case <-c.stopRefresh:
			return
		}
	}
}

func (c *Client) refreshPeers() error {
	conn, err := net.DialTimeout("tcp", c.seed, c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	request := encodePeer()
	if _, err := conn.Write(request); err != nil {
		return err
	}

	response := make([]byte, 4096)
	n, err := conn.Read(response)
	if err != nil {
		return err
	}

	status, _, data, err := decodeResponse(response[:n])
	if err != nil {
		return err
	}

	if status != 0x00 {
		return errors.New("peer refresh failed")
	}

	// Parse comma-separated peer list
	peerList := string(data)
	peers := make([]string, 0)
	for _, p := range strings.Split(peerList, ",") {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			peers = append(peers, trimmed)
		}
	}

	if len(peers) > 0 {
		c.mu.Lock()
		c.servers = peers
		c.rebuildHashRing()
		c.mu.Unlock()
	}

	return nil
}

func encodePeer() []byte {
	// PEER command is just 0x04 (matches Rust client's encode_peer())
	return []byte{0x04}
}

func (c *Client) selectServer(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch c.strategy {
	case RoundRobin:
		index := (atomic.AddUint64(&c.counter, 1) - 1) % uint64(len(c.servers))
		return c.servers[index]
	case ConsistentHashing:
		if c.hashRing == nil || len(c.hashRing.sortedHashes) == 0 {
			// Fallback to round robin if ring is empty
			index := (atomic.AddUint64(&c.counter, 1) - 1) % uint64(len(c.servers))
			return c.servers[index]
		}
		return c.hashRing.pickServer(key)
	default:
		return c.servers[0]
	}
}

func (c *Client) rebuildHashRing() {
	if c.strategy != ConsistentHashing {
		c.hashRing = nil
		return
	}

	ring := &consistentHashRing{
		sortedHashes:  make([]uint64, 0),
		serverIndices: make([]int, 0),
		servers:       make([]string, len(c.servers)),
		replicas:      150, // Match Rust client
	}

	// Copy servers to ring's server array
	copy(ring.servers, c.servers)

	// Build hash entries (hash, serverIndex pairs)
	type hashEntry struct {
		hash      uint64
		serverIdx int
	}
	entries := make([]hashEntry, 0, len(c.servers)*ring.replicas)

	for serverIdx, server := range c.servers {
		for i := 0; i < ring.replicas; i++ {
			virtualID := fmt.Sprintf("%s-%d", server, i)
			hash := fnvHash64(virtualID)
			entries = append(entries, hashEntry{hash: hash, serverIdx: serverIdx})
		}
	}

	// Sort by hash value
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].hash < entries[j].hash
	})

	// Build parallel arrays
	ring.sortedHashes = make([]uint64, len(entries))
	ring.serverIndices = make([]int, len(entries))
	for i, entry := range entries {
		ring.sortedHashes[i] = entry.hash
		ring.serverIndices[i] = entry.serverIdx
	}

	c.hashRing = ring
}

func (ring *consistentHashRing) pickServer(key string) string {
	if len(ring.sortedHashes) == 0 {
		return ""
	}

	keyHash := fnvHash64(key)

	// Binary search for first hash >= keyHash (O(log N) with better cache locality)
	idx := sort.Search(len(ring.sortedHashes), func(i int) bool {
		return ring.sortedHashes[i] >= keyHash
	})

	// If no hash >= keyHash, wrap around to first
	if idx >= len(ring.sortedHashes) {
		idx = 0
	}

	// Direct array access - no map lookup needed
	serverIdx := ring.serverIndices[idx]
	return ring.servers[serverIdx]
}

func fnvHash64(input string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(input))
	return h.Sum64()
}

func (c *Client) Get(key string) (*Item, error) {
	server := c.selectServer(key)

	conn, err := net.DialTimeout("tcp", server, c.timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := encodeRequest(0x01, key, nil)
	if _, err := conn.Write(request); err != nil {
		return nil, err
	}

	// Read status byte
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		return nil, err
	}
	status := statusBuf[0]

	switch status {
	case 0x00: // OK - read data length and data
		var dataLen uint32
		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			if _, err := io.ReadFull(conn, data); err != nil {
				return nil, err
			}
		}
		return &Item{Key: key, Value: data}, nil
	case 0x01: // ERROR - read message length and message
		var msgLen uint16
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			return nil, err
		}
		if msgLen > 0 {
			msgBytes := make([]byte, msgLen)
			if _, err := io.ReadFull(conn, msgBytes); err != nil {
				return nil, err
			}
			// Check if it's "not found" error
			if strings.Contains(strings.ToLower(string(msgBytes)), "not found") {
				return nil, ErrNotFound
			}
			return nil, errors.New("server error: " + string(msgBytes))
		}
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("unknown status: %d", status)
	}
}

func (c *Client) Set(item *Item) error {
	server := c.selectServer(item.Key)

	conn, err := net.DialTimeout("tcp", server, c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	request := encodeRequest(0x02, item.Key, item.Value)
	if _, err := conn.Write(request); err != nil {
		return err
	}

	// Read status byte
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		return err
	}
	status := statusBuf[0]

	if status == 0x00 {
		// OK response - read data length (should be 0 for PUT success)
		var dataLen uint32
		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		if dataLen > 0 {
			// Read and discard data
			discard := make([]byte, dataLen)
			io.ReadFull(conn, discard)
		}
		return nil
	} else if status == 0x01 {
		// ERROR - read message
		var msgLen uint16
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			return err
		}
		if msgLen > 0 {
			msgBytes := make([]byte, msgLen)
			if _, err := io.ReadFull(conn, msgBytes); err != nil {
				return err
			}
			return errors.New("set failed: " + string(msgBytes))
		}
		return errors.New("set failed")
	}
	return fmt.Errorf("unexpected status: %d", status)
}

func (c *Client) Delete(key string) error {
	server := c.selectServer(key)

	conn, err := net.DialTimeout("tcp", server, c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	request := encodeRequest(0x03, key, nil)
	if _, err := conn.Write(request); err != nil {
		return err
	}

	// Read status byte
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		return err
	}
	status := statusBuf[0]

	switch status {
	case 0x00:
		// OK - read data length (should be 0)
		var dataLen uint32
		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		if dataLen > 0 {
			discard := make([]byte, dataLen)
			io.ReadFull(conn, discard)
		}
		return nil
	case 0x01:
		// ERROR - check if it's "not found"
		var msgLen uint16
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			return err
		}
		if msgLen > 0 {
			msgBytes := make([]byte, msgLen)
			if _, err := io.ReadFull(conn, msgBytes); err != nil {
				return err
			}
			msg := strings.ToLower(string(msgBytes))
			if strings.Contains(msg, "not found") {
				return ErrNotFound
			}
			return errors.New("delete failed: " + string(msgBytes))
		}
		return ErrNotFound
	default:
		return fmt.Errorf("unexpected status: %d", status)
	}
}

func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	results := make(map[string]*Item)

	for _, key := range keys {
		item, err := c.Get(key)
		if err != nil && err != ErrNotFound {
			return nil, err
		}
		if item != nil {
			results[key] = item
		}
	}

	return results, nil
}

func (c *Client) Ping() error {
	if len(c.servers) == 0 {
		return errors.New("no servers configured")
	}

	conn, err := net.DialTimeout("tcp", c.servers[0], c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// PING is just command byte 0x00 (no key/data)
	request := []byte{0x00}
	if _, err := conn.Write(request); err != nil {
		return err
	}

	// Read status byte
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		return err
	}
	status := statusBuf[0]

	if status != 0x02 {
		return errors.New("ping failed")
	}

	return nil
}

func encodeRequest(command byte, key string, data []byte) []byte {
	var buf bytes.Buffer

	// Protocol format: [command:u8][key_len:u16][key:bytes][data_len:u32][data:bytes][ttl:u32?]
	// TTL is only for PUT (command 0x02)
	buf.WriteByte(command)
	binary.Write(&buf, binary.BigEndian, uint16(len(key)))
	buf.WriteString(key)
	binary.Write(&buf, binary.BigEndian, uint32(len(data)))
	buf.Write(data)

	// Add TTL for PUT commands (0 means use default/no TTL)
	if command == 0x02 {
		binary.Write(&buf, binary.BigEndian, uint32(0))
	}

	return buf.Bytes()
}

func decodeResponse(data []byte) (status byte, message string, responseData []byte, err error) {
	if len(data) < 1 {
		return 0, "", nil, errors.New("response too short")
	}

	buf := bytes.NewReader(data)

	// Read status byte first
	if err := binary.Read(buf, binary.BigEndian, &status); err != nil {
		return 0, "", nil, err
	}

	switch status {
	case 0x00: // OK
		// Format: [status:u8][data_len:u32][data:bytes]
		var dataLen uint32
		if err := binary.Read(buf, binary.BigEndian, &dataLen); err != nil {
			return 0, "", nil, err
		}
		if dataLen > 0 {
			responseData = make([]byte, dataLen)
			if _, err := buf.Read(responseData); err != nil {
				return 0, "", nil, err
			}
		}
		return status, "", responseData, nil

	case 0x01: // ERROR
		// Format: [status:u8][message_len:u16][message:bytes]
		var msgLen uint16
		if err := binary.Read(buf, binary.BigEndian, &msgLen); err != nil {
			return 0, "", nil, err
		}
		if msgLen > 0 {
			msgBytes := make([]byte, msgLen)
			if _, err := buf.Read(msgBytes); err != nil {
				return 0, "", nil, err
			}
			message = string(msgBytes)
		}
		return status, message, nil, nil

	case 0x02: // PONG
		return status, "", nil, nil

	default:
		return status, "", nil, fmt.Errorf("unknown status: %d", status)
	}
}
