package blazecache

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"sync/atomic"
)

// TLSClient is a TLS-enabled version of the BlazeCache client.
// It provides the same interface as Client but uses TLS encryption for all connections.
type TLSClient struct {
	*Client
	tlsConfig *tls.Config
}

// NewTLS creates a new TLS-enabled client with certificate verification.
//
// servers: List of server addresses in format "hostname:port"
//
// The client will verify server certificates using system root certificates.
func NewTLS(servers ...string) (*TLSClient, error) {
	if len(servers) == 0 {
		return nil, errors.New("at least one server required")
	}

	// Create TLS config with default root certificates
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		// Fallback to empty pool if system certs unavailable
		rootCAs = x509.NewCertPool()
	}

	tlsConfig := &tls.Config{
		RootCAs:            rootCAs,
		InsecureSkipVerify: false,
	}

	// Create underlying client
	client, err := New(servers...)
	if err != nil {
		return nil, err
	}

	return &TLSClient{
		Client:    client,
		tlsConfig: tlsConfig,
	}, nil
}

// NewTLSInsecure creates a TLS client that does not verify server certificates.
//
// **Warning**: This should only be used for development/testing with self-signed certificates.
// Production code should always verify certificates.
func NewTLSInsecure(servers ...string) (*TLSClient, error) {
	if len(servers) == 0 {
		return nil, errors.New("at least one server required")
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	// Create underlying client
	client, err := New(servers...)
	if err != nil {
		return nil, err
	}

	return &TLSClient{
		Client:    client,
		tlsConfig: tlsConfig,
	}, nil
}

// Override Get method to use TLS connections
func (c *TLSClient) Get(key string) (*Item, error) {
	server := c.selectServer(key)
	conn, err := c.getOrCreateConnection(server)
	if err != nil {
		return nil, err
	}
	shouldReturn := true
	defer func() {
		if shouldReturn && conn != nil {
			c.returnConnection(server, conn)
		} else if conn != nil {
			c.markConnectionDead(server)
		}
	}()

	// Use the base client's encoding/decoding logic
	request := encodeRequest(0x01, key, nil)
	if _, err := conn.Write(request); err != nil {
		shouldReturn = false
		return nil, err
	}

	response := make([]byte, 4096)
	n, err := conn.Read(response)
	if err != nil {
		shouldReturn = false
		return nil, err
	}

	status, _, data, err := decodeResponse(response[:n])
	if err != nil {
		shouldReturn = false
		return nil, err
	}

	switch status {
	case 0x00:
		return &Item{Key: key, Value: data}, nil
	case 0x01:
		return nil, ErrNotFound
	default:
		shouldReturn = false
		return nil, errors.New("server error")
	}
}

// Override Set method to use TLS connections
func (c *TLSClient) Set(item *Item) error {
	server := c.selectServer(item.Key)
	conn, err := c.getOrCreateConnection(server)
	if err != nil {
		return err
	}
	shouldReturn := true
	defer func() {
		if shouldReturn && conn != nil {
			c.returnConnection(server, conn)
		}
	}()

	request := encodeRequest(0x02, item.Key, item.Value)
	if _, err := conn.Write(request); err != nil {
		shouldReturn = false
		return err
	}

	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		shouldReturn = false
		return err
	}

	status, _, _, err := decodeResponse(response[:n])
	if err != nil {
		shouldReturn = false
		return err
	}

	if status != 0x00 {
		shouldReturn = false
		return errors.New("set failed")
	}
	return nil
}

// Override Delete method to use TLS connections
func (c *TLSClient) Delete(key string) error {
	server := c.selectServer(key)
	conn, err := c.getOrCreateConnection(server)
	if err != nil {
		return err
	}
	shouldReturn := true
	defer func() {
		if shouldReturn && conn != nil {
			c.returnConnection(server, conn)
		}
	}()

	request := encodeRequest(0x03, key, nil)
	if _, err := conn.Write(request); err != nil {
		shouldReturn = false
		return err
	}

	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		shouldReturn = false
		return err
	}

	status, _, _, err := decodeResponse(response[:n])
	if err != nil {
		shouldReturn = false
		return err
	}

	if status == 0x01 {
		return ErrNotFound
	}
	if status != 0x00 {
		shouldReturn = false
		return errors.New("delete failed")
	}
	return nil
}

// getOrCreateConnection creates TLS connections instead of plain TCP
func (c *TLSClient) getOrCreateConnection(server string) (net.Conn, error) {
	// Fast path: try to get from pool first
	poolChanVal, exists := c.connectionPool.Load(server)
	var poolChan chan net.Conn
	var poolCount *int32

	if !exists {
		// Initialize pool for this server
		poolChan = make(chan net.Conn, c.maxPoolSize)
		count := int32(0)
		poolCount = &count

		actualChan, _ := c.connectionPool.LoadOrStore(server, poolChan)
		poolChan = actualChan.(chan net.Conn)

		actualCount, _ := c.poolCounts.LoadOrStore(server, poolCount)
		poolCount = actualCount.(*int32)
	} else {
		poolChan = poolChanVal.(chan net.Conn)
		poolCountVal, _ := c.poolCounts.Load(server)
		poolCount = poolCountVal.(*int32)
	}

	// Try to get connection from pool (non-blocking)
	select {
	case conn := <-poolChan:
		// Check if connection is still alive
		if conn != nil {
			// Simple check: try to peek at connection state
			// If it's a TLS connection, we can check if it's closed
			return conn, nil
		}
	default:
		// Pool is empty, continue to create new connection
	}

	// Check if we can create a new connection
	current := atomic.LoadInt32(poolCount)
	if current < int32(c.maxPoolSize) {
		// Try to increment counter atomically
		if atomic.CompareAndSwapInt32(poolCount, current, current+1) {
			// Successfully claimed slot, create TLS connection
			conn, err := c.dialTLS(server)
			if err != nil {
				atomic.AddInt32(poolCount, -1)
				return nil, err
			}
			return conn, nil
		}
	}

	// Pool at max size or CAS failed, try pool one more time
	select {
	case conn := <-poolChan:
		if conn != nil {
			return conn, nil
		}
		// Fall through to create new connection
	default:
		// Pool empty, create new connection (allow overflow)
	}

	// Still nothing, create new connection (allow overflow)
	conn, err := c.dialTLS(server)
	if err != nil {
		return nil, err
	}
	atomic.AddInt32(poolCount, 1)
	return conn, nil
}

// dialTLS creates a new TLS connection to the server.
func (c *TLSClient) dialTLS(server string) (net.Conn, error) {
	// Create TLS dialer with config
	dialer := &net.Dialer{
		Timeout: connectionTimeout,
	}

	conn, err := tls.DialWithDialer(dialer, "tcp", server, c.tlsConfig)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// WithDiscovery creates a TLS client that discovers peers via the PEER command.
func TLSWithDiscovery(seed string, refreshSecs int) (*TLSClient, error) {
	if refreshSecs < 1 {
		refreshSecs = 1
	}

	// Create TLS config
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		rootCAs = x509.NewCertPool()
	}

	tlsConfig := &tls.Config{
		RootCAs:            rootCAs,
		InsecureSkipVerify: false,
	}

	// Create underlying client with discovery
	client, err := WithDiscovery(seed, refreshSecs)
	if err != nil {
		return nil, err
	}

	return &TLSClient{
		Client:    client,
		tlsConfig: tlsConfig,
	}, nil
}

