package com.blazecache;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UDP Client with QUIC-like features for high-performance message handling.
 * 
 * Features:
 * - Fragmentation and reassembly for large messages (QUIC datagram splitting)
 * - Request ID-based multiplexing (QUIC connection ID)
 * - Fast path for small single-datagram messages (QUIC 0-RTT optimization)
 * - Automatic retry logic with timeout handling
 * 
 * Protocol Format:
 * Single-datagram: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][CMD:1][DATA:...]
 * Fragment: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][SEQ:2][FRAG_COUNT:2][PAYLOAD_LEN:2][DATA:...]
 */
public class UDPClient implements AutoCloseable {
    // UDP protocol constants (matching Rust/Go implementations)
    private static final int UDP_MAGIC = 0xBC01;
    private static final byte UDP_VERSION = 1;
    private static final int UDP_MAX_DATAGRAM = 1200; // QUIC-safe MTU
    private static final int UDP_MAX_PAYLOAD = UDP_MAX_DATAGRAM - 14; // Fragment header size
    private static final int UDP_MAX_MESSAGE_BYTES = 4 * 1024 * 1024; // 4MB max message
    private static final int UDP_HEADER_LEN = 14; // Fragment header length
    private static final int UDP_SINGLE_HEADER_LEN = 9; // Single datagram header length
    private static final long UDP_REASSEMBLY_TIMEOUT_MS = 2000; // 2 seconds
    private static final long UDP_CLIENT_TIMEOUT_MS = 5000; // 5 seconds
    
    // Message type flags
    private static final byte UDP_FLAG_REQUEST = 0;
    private static final byte UDP_FLAG_RESPONSE = 1;
    
    // Commands
    private static final byte CMD_GET = 0x01;
    private static final byte CMD_PUT = 0x02;
    private static final byte CMD_DELETE = 0x03;
    private static final byte CMD_PING = 0x04;
    
    // Response status codes
    private static final byte STATUS_OK = 0x00;
    private static final byte STATUS_NOT_FOUND = 0x01;
    private static final byte STATUS_ERROR = 0x02;
    
    private final DatagramSocket socket;
    private final InetSocketAddress serverAddr;
    private final AtomicLong requestID = new AtomicLong(0);
    private final Map<Long, ReassemblyState> reassemblyMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor = Executors.newScheduledThreadPool(1);
    
    /**
     * Creates a new UDP client connected to the specified server.
     * 
     * @param serverAddr Server address in format "host:port"
     * @throws IOException if the socket cannot be created
     */
    public UDPClient(String serverAddr) throws IOException {
        String[] parts = serverAddr.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid server address format: " + serverAddr);
        }
        
        // Force IPv4 resolution (like Go client)
        InetAddress addr = InetAddress.getByName(parts[0]);
        this.serverAddr = new InetSocketAddress(addr, Integer.parseInt(parts[1]));
        
        // Create UDP socket with optimized settings (bind to any available port)
        // Like Go client, we bind to a local port so server can send responses back
        // Use the same approach as debug program: create socket and let it bind to a random port
        this.socket = new DatagramSocket();
        this.socket.setSoTimeout((int) UDP_CLIENT_TIMEOUT_MS);
        
        // Set socket buffer sizes for high throughput (QUIC-like optimization)
        try {
            this.socket.setReceiveBufferSize(4 * 1024 * 1024); // 4MB receive buffer
            this.socket.setSendBufferSize(4 * 1024 * 1024); // 4MB send buffer
        } catch (SocketException e) {
            // Ignore if system doesn't support large buffers
        }
        
        // Small delay to ensure socket is ready (like Rust client)
        // Increased delay for Docker networking to stabilize
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Start cleanup task for expired reassembly entries
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpiredReassembly, 
            1, 1, TimeUnit.SECONDS);
    }
    
    /**
     * Gets a value from the cache.
     * 
     * @param key The key to retrieve
     * @return Optional containing the value if found, empty otherwise
     * @throws IOException if the operation fails
     */
    public Optional<byte[]> get(String key) throws IOException {
        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] requestData = encodeCommand(CMD_GET, keyBytes, null);
        byte[] response = sendRequest(CMD_GET, requestData);
        
        if (response.length == 0) {
            return Optional.empty();
        }
        
        // Response format: [STATUS:1][DATA_LEN:4][DATA:...]
        if (response[0] == STATUS_OK) {
            if (response.length < 5) {
                return Optional.empty();
            }
            int dataLen = ByteBuffer.wrap(response, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt();
            if (dataLen > 0 && response.length >= 5 + dataLen) {
                byte[] data = new byte[dataLen];
                System.arraycopy(response, 5, data, 0, dataLen);
                return Optional.of(data);
            }
            return Optional.empty();
        } else if (response[0] == STATUS_NOT_FOUND) {
            return Optional.empty();
        } else {
            throw new IOException("GET failed with status: " + response[0]);
        }
    }
    
    /**
     * Sets a value in the cache.
     * 
     * @param key The key to set
     * @param value The value to store
     * @throws IOException if the operation fails
     */
    public void set(String key, byte[] value) throws IOException {
        set(key, value, 0); // TTL = 0 (no expiration)
    }
    
    /**
     * Sets a value in the cache with TTL.
     * 
     * @param key The key to set
     * @param value The value to store
     * @param ttlSeconds Time to live in seconds (0 = no expiration)
     * @throws IOException if the operation fails
     */
    public void set(String key, byte[] value, int ttlSeconds) throws IOException {
        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] requestData = encodeCommand(CMD_PUT, keyBytes, value, ttlSeconds);
        byte[] response = sendRequest(CMD_PUT, requestData);
        
        if (response.length == 0 || response[0] != STATUS_OK) {
            throw new IOException("PUT failed with status: " + 
                (response.length > 0 ? response[0] : "empty"));
        }
    }
    
    /**
     * Deletes a value from the cache.
     * 
     * @param key The key to delete
     * @return true if the key was deleted, false if not found
     * @throws IOException if the operation fails
     */
    public boolean delete(String key) throws IOException {
        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] requestData = encodeCommand(CMD_DELETE, keyBytes, null);
        byte[] response = sendRequest(CMD_DELETE, requestData);
        
        if (response.length == 0) {
            return false;
        }
        
        if (response[0] == STATUS_OK) {
            return true;
        } else if (response[0] == STATUS_NOT_FOUND) {
            return false;
        } else {
            throw new IOException("DELETE failed with status: " + response[0]);
        }
    }
    
    /**
     * Pings the server to check connectivity.
     * 
     * @throws IOException if the ping fails
     */
    public void ping() throws IOException {
        byte[] requestData = encodeCommand(CMD_PING, null, null); // Empty data for PING
        byte[] response = sendRequest(CMD_PING, requestData);
        
        if (response.length == 0 || response[0] != STATUS_OK) {
            throw new IOException("PING failed");
        }
    }
    
    /**
     * Closes the UDP socket and cleanup resources.
     */
    @Override
    public void close() {
        cleanupExecutor.shutdown();
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
    
    // Private helper methods
    
    private long nextRequestID() {
        return requestID.incrementAndGet() & 0xFFFFFFFFL; // Keep as 32-bit
    }
    
    private byte[] encodeCommand(byte command, byte[] key, byte[] value) {
        return encodeCommand(command, key, value, 0);
    }
    
    private byte[] encodeCommand(byte command, byte[] key, byte[] value, int ttl) {
        // For single datagram, the command byte goes at position 8 (after header)
        // So we return just the command data (without command byte for single datagram)
        // The command byte will be placed directly in sendSingleDatagram
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        try {
            // Key length (2 bytes) + key
            if (key != null) {
                dos.writeShort(key.length);
                dos.write(key);
            }
            
            // For PUT: value length (4 bytes) + value + TTL (4 bytes)
            if (command == CMD_PUT && value != null) {
                dos.writeInt(value.length);
                dos.write(value);
                dos.writeInt(ttl);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode command", e);
        }
        
        return baos.toByteArray();
    }
    
    private byte[] sendRequest(byte command, byte[] requestData) throws IOException {
        long reqID = nextRequestID();
        
        // Check if message fits in single datagram (fast path)
        if (requestData.length <= UDP_MAX_PAYLOAD) {
            return sendSingleDatagram(reqID, command, requestData);
        } else {
            // For fragmented, we need to include command byte in the data
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(command);
            baos.write(requestData, 0, requestData.length);
            return sendFragmented(reqID, baos.toByteArray());
        }
    }
    
    private byte[] sendSingleDatagram(long reqID, byte command, byte[] data) throws IOException {
        // Format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][CMD:1][DATA:...]
        // The server expects the command byte at position 8 (after request ID)
        ByteBuffer packet = ByteBuffer.allocate(UDP_SINGLE_HEADER_LEN + data.length)
            .order(ByteOrder.BIG_ENDIAN);
        
        packet.putShort((short) UDP_MAGIC);
        packet.put(UDP_VERSION);
        packet.put(UDP_FLAG_REQUEST);
        packet.putInt((int) reqID);
        packet.put(command); // Command byte at position 8
        packet.put(data); // Command data after command byte
        
        byte[] packetBytes = packet.array();
        DatagramPacket datagram = new DatagramPacket(
            packetBytes, packetBytes.length, serverAddr);
        
        socket.send(datagram);
        
        // Small delay to ensure packet is sent before waiting for response
        // This helps ensure the socket is ready to receive
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return receiveResponse(reqID, false);
    }
    
    private byte[] sendFragmented(long reqID, byte[] data) throws IOException {
        if (data.length > UDP_MAX_MESSAGE_BYTES) {
            throw new IOException("Message too large: " + data.length + " bytes");
        }
        
        int fragCount = (data.length + UDP_MAX_PAYLOAD - 1) / UDP_MAX_PAYLOAD;
        if (fragCount > 65535) {
            throw new IOException("Too many fragments: " + fragCount);
        }
        
        // Send all fragments
        for (int i = 0; i < fragCount; i++) {
            int start = i * UDP_MAX_PAYLOAD;
            int end = Math.min(start + UDP_MAX_PAYLOAD, data.length);
            byte[] payload = Arrays.copyOfRange(data, start, end);
            
            // Format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][SEQ:2][FRAG_COUNT:2][PAYLOAD_LEN:2][DATA:...]
            ByteBuffer packet = ByteBuffer.allocate(UDP_HEADER_LEN + payload.length)
                .order(ByteOrder.BIG_ENDIAN);
            
            packet.putShort((short) UDP_MAGIC);
            packet.put(UDP_VERSION);
            packet.put(UDP_FLAG_REQUEST);
            packet.putInt((int) reqID);
            packet.putShort((short) i); // Sequence number
            packet.putShort((short) fragCount); // Fragment count
            packet.putShort((short) payload.length); // Payload length
            packet.put(payload);
            
            byte[] packetBytes = packet.array();
            DatagramPacket datagram = new DatagramPacket(
                packetBytes, packetBytes.length, serverAddr);
            
            socket.send(datagram);
        }
        
        return receiveResponse(reqID, true);
    }
    
    private byte[] receiveResponse(long reqID, boolean fragmented) throws IOException {
        long deadline = System.currentTimeMillis() + UDP_CLIENT_TIMEOUT_MS;
        ReassemblyState reassembly = fragmented ? new ReassemblyState() : null;
        byte[] buffer = new byte[UDP_MAX_DATAGRAM];
        
        // Clean up any expired reassembly states before starting
        cleanupExpiredReassembly();
        
        // Reset socket timeout to default before starting to receive
        try {
            socket.setSoTimeout((int) UDP_CLIENT_TIMEOUT_MS);
        } catch (SocketException e) {
            throw new IOException("Socket error: " + e.getMessage(), e);
        }
        
        while (System.currentTimeMillis() < deadline) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            
            // Set timeout for this receive attempt
            // Use a reasonable minimum timeout (100ms) to avoid too many iterations
            int timeoutMs = (int) Math.max(Math.min(remaining, UDP_CLIENT_TIMEOUT_MS), 100);
            try {
                socket.setSoTimeout(timeoutMs);
            } catch (SocketException e) {
                // Socket might be closed, break out
                break;
            }
            
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (SocketTimeoutException e) {
                // Timeout on this iteration - check if we still have time overall
                if (System.currentTimeMillis() >= deadline) {
                    break;
                }
                continue;
            } catch (SocketException e) {
                // Socket error, break out
                throw new IOException("Socket error while receiving: " + e.getMessage(), e);
            }
            
            byte[] received = Arrays.copyOf(packet.getData(), packet.getLength());
            
            // Early validation: check packet length
            if (received.length < UDP_SINGLE_HEADER_LEN) {
                continue; // Too short, skip
            }
            
            // Early validation: check magic number (bytes 0-1)
            int magic = ((received[0] & 0xFF) << 8) | (received[1] & 0xFF);
            if (magic != UDP_MAGIC) {
                continue; // Wrong magic, skip
            }
            
            // Early validation: check version (byte 2)
            byte version = received[2];
            if (version != UDP_VERSION) {
                continue; // Wrong version, skip
            }
            
            // Early validation: check flags (must be response) (byte 3)
            byte flags = received[3];
            if (flags != UDP_FLAG_RESPONSE) {
                continue; // Not a response, skip
            }
            
            // Extract request ID (bytes 4-7) - big-endian
            int recvReqID = ((received[4] & 0xFF) << 24) |
                           ((received[5] & 0xFF) << 16) |
                           ((received[6] & 0xFF) << 8) |
                           (received[7] & 0xFF);
            
            // Check request ID match (convert both to int for comparison)
            if (recvReqID != (int) reqID) {
                continue; // Wrong request ID, continue waiting
            }
            
            // Got the right response! Now parse it
            // Check if it's a single datagram (byte 8 is status) or fragment (bytes 8-9 are seq_no)
            // Single datagram: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][STATUS:1][DATA:...]
            // Fragment: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][SEQ:2][FRAG_COUNT:2][PAYLOAD_LEN:2][DATA:...]
            // For single datagram, byte 8 is the status (0x00-0x02)
            // For fragments, bytes 8-9 are seq_no (u16), which could be 0-65535
            // If byte 8 is <= 0x02, it's likely a status byte (single datagram)
            
            if (received.length < UDP_SINGLE_HEADER_LEN) {
                continue; // Too short for single datagram
            }
            
            // Check byte 8 to determine if single datagram or fragment
            byte byte8 = received[8];
            
            // If byte 8 is <= 0x02 and we have enough bytes, it's likely a single datagram
            // (status codes are 0x00, 0x01, 0x02)
            // For PING, response is exactly 9 bytes (header only, no data)
            if (byte8 <= 0x02 && received.length >= UDP_SINGLE_HEADER_LEN) {
                // Single datagram response
                byte status = byte8;
                byte[] data;
                if (received.length > UDP_SINGLE_HEADER_LEN) {
                    data = Arrays.copyOfRange(received, UDP_SINGLE_HEADER_LEN, received.length);
                } else {
                    data = new byte[0]; // No data (e.g., PING response is exactly 9 bytes)
                }
                
                // Prepend status byte for consistency with TCP client's decodeResponse
                byte[] response = new byte[1 + data.length];
                response[0] = status;
                if (data.length > 0) {
                    System.arraycopy(data, 0, response, 1, data.length);
                }
                
                return response;
            } else if (received.length >= UDP_HEADER_LEN) {
                // Likely fragment - decode fragment header
                // Fragment format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][SEQ:2][FRAG_COUNT:2][PAYLOAD_LEN:2][DATA:...]
                // Bytes 8-9: seq_no (u16)
                // Bytes 10-11: frag_count (u16)
                // Bytes 12-13: payload_len (u16)
                short seqNo = (short) (((received[8] & 0xFF) << 8) | (received[9] & 0xFF));
                short fragCount = (short) (((received[10] & 0xFF) << 8) | (received[11] & 0xFF));
                
                if (fragCount > 0 && seqNo < fragCount) {
                    short payloadLen = (short) (((received[12] & 0xFF) << 8) | (received[13] & 0xFF));
                    if (payloadLen > 0 && received.length >= UDP_HEADER_LEN + payloadLen) {
                        byte[] payload = Arrays.copyOfRange(received, UDP_HEADER_LEN, UDP_HEADER_LEN + payloadLen);
                        
                        if (reassembly == null) {
                            reassembly = new ReassemblyState();
                            reassemblyMap.put(reqID, reassembly);
                        }
                        
                        reassembly.addFragment(seqNo, fragCount, payload);
                        
                        if (reassembly.isComplete()) {
                            reassemblyMap.remove(reqID);
                            return reassembly.assemble();
                        }
                    }
                }
            }
        }
        
        reassemblyMap.remove(reqID);
        throw new IOException("Response timeout for request ID: " + reqID);
    }
    
    private void cleanupExpiredReassembly() {
        long now = System.currentTimeMillis();
        reassemblyMap.entrySet().removeIf(entry -> {
            ReassemblyState state = entry.getValue();
            return (now - state.createdAt) > UDP_REASSEMBLY_TIMEOUT_MS;
        });
    }
    
    /**
     * Tracks fragments for reassembly (QUIC datagram reassembly).
     */
    private static class ReassemblyState {
        private final Map<Integer, byte[]> fragments = new ConcurrentHashMap<>();
        private final AtomicInteger receivedCount = new AtomicInteger(0);
        private final long createdAt = System.currentTimeMillis();
        private volatile int totalFragments = -1;
        
        void addFragment(int seqNo, int fragCount, byte[] payload) {
            if (totalFragments == -1) {
                totalFragments = fragCount;
            }
            
            if (fragments.putIfAbsent(seqNo, payload) == null) {
                receivedCount.incrementAndGet();
            }
        }
        
        boolean isComplete() {
            return totalFragments > 0 && receivedCount.get() == totalFragments;
        }
        
        byte[] assemble() {
            if (!isComplete()) {
                throw new IllegalStateException("Reassembly not complete");
            }
            
            // Calculate total size
            int totalSize = 0;
            for (int i = 0; i < totalFragments; i++) {
                byte[] frag = fragments.get(i);
                if (frag == null) {
                    throw new IllegalStateException("Missing fragment: " + i);
                }
                totalSize += frag.length;
            }
            
            // Assemble in order
            byte[] result = new byte[totalSize];
            int offset = 0;
            for (int i = 0; i < totalFragments; i++) {
                byte[] frag = fragments.get(i);
                System.arraycopy(frag, 0, result, offset, frag.length);
                offset += frag.length;
            }
            
            // Parse response: [STATUS:1][DATA:...]
            if (result.length == 0) {
                return new byte[]{STATUS_ERROR};
            }
            
            return result;
        }
    }
}

