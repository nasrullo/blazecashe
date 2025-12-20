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
	"unsafe"
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

// serverSelection is a snapshot used for lock-free reads
type serverSelection struct {
	strategy SelectionStrategy
	servers  []string
	hashRing *consistentHashRing
}

type Client struct {
	servers     []string
	strategy    SelectionStrategy
	counter     uint64
	timeout     time.Duration
	hashRing    *consistentHashRing
	mu          sync.RWMutex // Only for writes (WithStrategy, refreshPeers)
	seed        string        // for discovery mode
	refreshSecs int           // for discovery mode
	stopRefresh chan struct{} // to stop the refresh goroutine
	// Connection pooling (optimized for high throughput - lock-free reads)
	connectionPool *sync.Map                // server -> chan net.Conn (lock-free reads)
	poolCounts     *sync.Map                // server -> *int32 (lock-free reads)
	maxPoolSize    int                      // maximum connections per server
	// Lock-free reads using RCU pattern
	selection unsafe.Pointer // atomic pointer to *serverSelection
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

const (
	maxPoolSize       = 500 // Maximum connections per server (increased for high concurrency)
	connectionTimeout = 5 * time.Second
)

func New(servers ...string) (*Client, error) {
	if len(servers) == 0 {
		return nil, errors.New("at least one server required")
	}

	c := &Client{
		servers:        servers,
		strategy:       RoundRobin,
		timeout:        5 * time.Second,
		connectionPool: &sync.Map{},
		poolCounts:     &sync.Map{},
		maxPoolSize:    maxPoolSize,
	}
	c.rebuildHashRing()
	// Initialize lock-free selection snapshot
	c.updateSelectionSnapshot()
	return c, nil
}

func (c *Client) updateSelectionSnapshot() {
	// Create a new snapshot with current values
	snapshot := &serverSelection{
		strategy: c.strategy,
		servers:  make([]string, len(c.servers)),
		hashRing: c.hashRing,
	}
	copy(snapshot.servers, c.servers)
	// Atomically update the pointer
	atomic.StorePointer(&c.selection, unsafe.Pointer(snapshot))
}

func (c *Client) WithStrategy(strategy SelectionStrategy, weights ...uint32) *Client {
	c.mu.Lock()
	c.strategy = strategy
	c.rebuildHashRing()
	c.updateSelectionSnapshot()
	c.mu.Unlock()
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
		servers:        []string{seed},
		strategy:       ConsistentHashing,
		timeout:        5 * time.Second,
		seed:           seed,
		refreshSecs:    refreshSecs,
		stopRefresh:    make(chan struct{}),
		connectionPool: &sync.Map{},
		poolCounts:     &sync.Map{},
		maxPoolSize:    maxPoolSize,
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
		c.updateSelectionSnapshot()
		c.mu.Unlock()
	}

	return nil
}

func encodePeer() []byte {
	// PEER command is just 0x04 (matches Rust client's encode_peer())
	return []byte{0x04}
}

func (c *Client) selectServer(key string) string {
	// Lock-free read using RCU pattern
	selectionPtr := (*serverSelection)(atomic.LoadPointer(&c.selection))
	
	if selectionPtr == nil {
		// Fallback to locked read if snapshot not initialized
		c.mu.RLock()
		strategy := c.strategy
		servers := c.servers
		hashRing := c.hashRing
		c.mu.RUnlock()
		return c.selectServerWithValues(key, strategy, servers, hashRing)
	}
	
	// Use snapshot values (no lock needed)
	return c.selectServerWithValues(key, selectionPtr.strategy, selectionPtr.servers, selectionPtr.hashRing)
}

func (c *Client) selectServerWithValues(key string, strategy SelectionStrategy, servers []string, hashRing *consistentHashRing) string {
	switch strategy {
	case RoundRobin:
		index := (atomic.AddUint64(&c.counter, 1) - 1) % uint64(len(servers))
		return servers[index]
	case ConsistentHashing:
		if hashRing == nil || len(hashRing.sortedHashes) == 0 {
			// Fallback to round robin if ring is empty
			index := (atomic.AddUint64(&c.counter, 1) - 1) % uint64(len(servers))
			return servers[index]
		}
		return hashRing.pickServer(key)
	default:
		return servers[0]
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

// getOrCreateConnection gets a connection from the pool or creates a new one
// Uses Go's channel-based pattern for efficient connection reuse
// Optimized with sync.Map for lock-free reads
func (c *Client) getOrCreateConnection(server string) (net.Conn, error) {
	// Fast path: lock-free read using sync.Map
	poolChanVal, exists := c.connectionPool.Load(server)
	var poolChan chan net.Conn
	var poolCount *int32
	
	if !exists {
		// Initialize pool for this server (only happens once per server)
		poolChan = make(chan net.Conn, c.maxPoolSize)
		count := int32(0)
		poolCount = &count
		
		// Use LoadOrStore to ensure only one goroutine initializes
		actualChan, _ := c.connectionPool.LoadOrStore(server, poolChan)
		poolChan = actualChan.(chan net.Conn)
		
		actualCount, _ := c.poolCounts.LoadOrStore(server, poolCount)
		poolCount = actualCount.(*int32)
	} else {
		poolChan = poolChanVal.(chan net.Conn)
		if countVal, ok := c.poolCounts.Load(server); ok {
			poolCount = countVal.(*int32)
		} else {
			// Shouldn't happen, but handle it
			count := int32(0)
			poolCount = &count
			c.poolCounts.Store(server, poolCount)
		}
	}

	// Try to get a connection from the channel (non-blocking)
	select {
	case conn := <-poolChan:
		// Got a connection from the pool - return it immediately
		return conn, nil
	default:
		// No connection available, check if we can create a new one
		currentCount := atomic.LoadInt32(poolCount)
		if currentCount < int32(c.maxPoolSize) {
			// Try to increment the counter atomically
			if atomic.CompareAndSwapInt32(poolCount, currentCount, currentCount+1) {
				// Successfully claimed a slot, create new connection
				conn, err := net.DialTimeout("tcp", server, connectionTimeout)
				if err != nil {
					// Failed to create, decrement counter
					atomic.AddInt32(poolCount, -1)
					return nil, err
				}
				// Optimize TCP settings for low latency
				if tcpConn, ok := conn.(*net.TCPConn); ok {
					tcpConn.SetNoDelay(true) // Disable Nagle's algorithm for low latency
				}
				return conn, nil
			}
			// CAS failed, another goroutine got it - try channel again
			select {
			case conn := <-poolChan:
				return conn, nil
			default:
				// Still nothing, create new connection (allow overflow)
				conn, err := net.DialTimeout("tcp", server, connectionTimeout)
				if err != nil {
					return nil, err
				}
				// Optimize TCP settings for low latency
				if tcpConn, ok := conn.(*net.TCPConn); ok {
					tcpConn.SetNoDelay(true) // Disable Nagle's algorithm for low latency
				}
				atomic.AddInt32(poolCount, 1)
				return conn, nil
			}
		}
		
		// Pool is at max size, try channel one more time (non-blocking)
		select {
		case conn := <-poolChan:
			return conn, nil
		default:
			// Still no connection available, create new one (allow overflow to prevent blocking)
			conn, err := net.DialTimeout("tcp", server, connectionTimeout)
			if err != nil {
				return nil, err
			}
			// Optimize TCP settings for low latency
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				tcpConn.SetNoDelay(true) // Disable Nagle's algorithm for low latency
			}
			atomic.AddInt32(poolCount, 1)
			return conn, nil
		}
	}
}

// returnConnection returns a connection to the pool
// Uses channels for efficient goroutine synchronization (lock-free)
func (c *Client) returnConnection(server string, conn net.Conn) {
	if conn == nil {
		return
	}

	// Lock-free read using sync.Map
	poolChanVal, exists := c.connectionPool.Load(server)
	if !exists {
		// Pool doesn't exist, just close the connection
		conn.Close()
		return
	}

	poolChan := poolChanVal.(chan net.Conn)

	// Try non-blocking send first (fast path)
	select {
	case poolChan <- conn:
		// Successfully returned to pool (lock-free)
		return
	default:
		// Channel full, try with brief timeout to avoid dropping connections
		select {
		case poolChan <- conn:
			// Successfully returned to pool
			return
		case <-time.After(1 * time.Millisecond):
			// Timeout - pool is truly full, close connection
			conn.Close()
			// Decrement counter atomically (lock-free)
			if poolCountVal, ok := c.poolCounts.Load(server); ok {
				poolCount := poolCountVal.(*int32)
				atomic.AddInt32(poolCount, -1)
			}
		}
	}
}

// markConnectionDead marks a connection as dead and decrements the pool count
func (c *Client) markConnectionDead(server string) {
	// Lock-free read using sync.Map
	if poolCountVal, ok := c.poolCounts.Load(server); ok {
		poolCount := poolCountVal.(*int32)
		atomic.AddInt32(poolCount, -1)
	}
}

func (c *Client) Get(key string) (*Item, error) {
	server := c.selectServer(key)

	conn, err := c.getOrCreateConnection(server)
	if err != nil {
		return nil, err
	}
	// Track if we should return connection (set to false on error)
	shouldReturn := true
	defer func() {
		if shouldReturn && conn != nil {
			c.returnConnection(server, conn)
		} else if conn != nil {
			// Connection was closed due to error, just mark as dead
			c.markConnectionDead(server)
		}
	}()

	request := encodeRequest(0x01, key, nil)
	if _, err := conn.Write(request); err != nil {
		conn.Close()
		shouldReturn = false
		return nil, err
	}

	// Read status byte
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		conn.Close()
		shouldReturn = false
		return nil, err
	}
	status := statusBuf[0]

	switch status {
	case 0x00: // OK - read data length and data
		var dataLen uint32
		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			conn.Close()
			shouldReturn = false
			return nil, err
		}
		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			if _, err := io.ReadFull(conn, data); err != nil {
				conn.Close()
				shouldReturn = false
				return nil, err
			}
		}
		// Success - connection will be returned to pool by defer
		return &Item{Key: key, Value: data}, nil
	case 0x01: // ERROR - read message length and message
		var msgLen uint16
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			conn.Close()
			shouldReturn = false
			return nil, err
		}
		if msgLen > 0 {
			msgBytes := make([]byte, msgLen)
			if _, err := io.ReadFull(conn, msgBytes); err != nil {
				conn.Close()
				shouldReturn = false
				return nil, err
			}
			// Check if it's "not found" error - connection is still good, will be returned by defer
			if strings.Contains(strings.ToLower(string(msgBytes)), "not found") {
				return nil, ErrNotFound
			}
			return nil, errors.New("server error: " + string(msgBytes))
		}
		return nil, ErrNotFound
	default:
		conn.Close()
		shouldReturn = false
		return nil, fmt.Errorf("unknown status: %d", status)
	}
}

func (c *Client) Set(item *Item) error {
	server := c.selectServer(item.Key)

	conn, err := c.getOrCreateConnection(server)
	if err != nil {
		return err
	}
	// Track if we should return connection (set to false on error)
	shouldReturn := true
	defer func() {
		if shouldReturn && conn != nil {
			c.returnConnection(server, conn)
		}
	}()

	request := encodeRequest(0x02, item.Key, item.Value)
	if _, err := conn.Write(request); err != nil {
		conn.Close()
		c.markConnectionDead(server)
		shouldReturn = false
		return err
	}

	// Read status byte
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		conn.Close()
		c.markConnectionDead(server)
		shouldReturn = false
		return err
	}
	status := statusBuf[0]

	if status == 0x00 {
		// OK response - read data length (should be 0 for PUT success)
		var dataLen uint32
		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			conn.Close()
			shouldReturn = false
			return err
		}
		if dataLen > 0 {
			// Read and discard data
			discard := make([]byte, dataLen)
			if _, err := io.ReadFull(conn, discard); err != nil {
				conn.Close()
				shouldReturn = false
				return err
			}
		}
		// Success - connection will be returned by defer
		return nil
	} else if status == 0x01 {
		// ERROR - read message (connection is still good for protocol errors)
		var msgLen uint16
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			conn.Close()
			shouldReturn = false
			return err
		}
		if msgLen > 0 {
			msgBytes := make([]byte, msgLen)
			if _, err := io.ReadFull(conn, msgBytes); err != nil {
				conn.Close()
				shouldReturn = false
				return err
			}
			return errors.New("set failed: " + string(msgBytes))
		}
		return errors.New("set failed")
	}
	conn.Close()
	shouldReturn = false
	return fmt.Errorf("unexpected status: %d", status)
}

func (c *Client) Delete(key string) error {
	server := c.selectServer(key)

	conn, err := c.getOrCreateConnection(server)
	if err != nil {
		return err
	}
	// Track if we should return connection (set to false on error)
	shouldReturn := true
	defer func() {
		if shouldReturn && conn != nil {
			c.returnConnection(server, conn)
		}
	}()

	request := encodeRequest(0x03, key, nil)
	if _, err := conn.Write(request); err != nil {
		conn.Close()
		shouldReturn = false
		return err
	}

	// Read status byte
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		conn.Close()
		shouldReturn = false
		return err
	}
	status := statusBuf[0]

	switch status {
	case 0x00:
		// OK - read data length (should be 0)
		var dataLen uint32
		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			conn.Close()
			shouldReturn = false
			return err
		}
		if dataLen > 0 {
			discard := make([]byte, dataLen)
			if _, err := io.ReadFull(conn, discard); err != nil {
				conn.Close()
				shouldReturn = false
				return err
			}
		}
		// Success - connection will be returned by defer
		return nil
	case 0x01:
		// ERROR - check if it's "not found" (connection is still good for protocol errors)
		var msgLen uint16
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			conn.Close()
			shouldReturn = false
			return err
		}
		if msgLen > 0 {
			msgBytes := make([]byte, msgLen)
			if _, err := io.ReadFull(conn, msgBytes); err != nil {
				conn.Close()
				shouldReturn = false
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
		conn.Close()
		shouldReturn = false
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

	server := c.servers[0]
	conn, err := c.getOrCreateConnection(server)
	if err != nil {
		return err
	}
	// Track if we should return connection (set to false on error)
	shouldReturn := true
	defer func() {
		if shouldReturn && conn != nil {
			c.returnConnection(server, conn)
		}
	}()

	// PING is just command byte 0x00 (no key/data)
	request := []byte{0x00}
	if _, err := conn.Write(request); err != nil {
		conn.Close()
		c.markConnectionDead(server)
		shouldReturn = false
		return err
	}

	// Read status byte
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		conn.Close()
		c.markConnectionDead(server)
		shouldReturn = false
		return err
	}
	status := statusBuf[0]

	if status != 0x02 {
		conn.Close()
		c.markConnectionDead(server)
		shouldReturn = false
		return errors.New("ping failed")
	}

	// Success - connection will be returned by defer
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
