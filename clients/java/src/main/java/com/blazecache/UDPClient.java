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
        
        this.serverAddr = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
        
        // Create UDP socket with optimized settings
        this.socket = new DatagramSocket();
        this.socket.setSoTimeout((int) UDP_CLIENT_TIMEOUT_MS);
        
        // Set socket buffer sizes for high throughput (QUIC-like optimization)
        try {
            this.socket.setReceiveBufferSize(4 * 1024 * 1024); // 4MB receive buffer
            this.socket.setSendBufferSize(4 * 1024 * 1024); // 4MB send buffer
        } catch (SocketException e) {
            // Ignore if system doesn't support large buffers
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
        byte[] response = sendRequest(requestData);
        
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
        byte[] response = sendRequest(requestData);
        
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
        byte[] response = sendRequest(requestData);
        
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
        byte[] requestData = encodeCommand(CMD_PING, null, null);
        byte[] response = sendRequest(requestData);
        
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        try {
            // Command byte
            dos.writeByte(command);
            
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
    
    private byte[] sendRequest(byte[] requestData) throws IOException {
        long reqID = nextRequestID();
        
        // Check if message fits in single datagram (fast path)
        if (requestData.length <= UDP_MAX_PAYLOAD - 1) { // -1 for command byte in single header
            return sendSingleDatagram(reqID, requestData);
        } else {
            return sendFragmented(reqID, requestData);
        }
    }
    
    private byte[] sendSingleDatagram(long reqID, byte[] data) throws IOException {
        // Format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][CMD_DATA:...]
        ByteBuffer packet = ByteBuffer.allocate(UDP_SINGLE_HEADER_LEN + data.length)
            .order(ByteOrder.BIG_ENDIAN);
        
        packet.putShort((short) UDP_MAGIC);
        packet.put(UDP_VERSION);
        packet.put(UDP_FLAG_REQUEST);
        packet.putInt((int) reqID);
        packet.put(data);
        
        byte[] packetBytes = packet.array();
        DatagramPacket datagram = new DatagramPacket(
            packetBytes, packetBytes.length, serverAddr);
        
        socket.send(datagram);
        
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
        
        while (System.currentTimeMillis() < deadline) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            
            socket.setSoTimeout((int) Math.min(remaining, UDP_CLIENT_TIMEOUT_MS));
            
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (SocketTimeoutException e) {
                continue;
            }
            
            byte[] received = Arrays.copyOf(packet.getData(), packet.getLength());
            
            // Check if it's a single datagram or fragment
            if (received.length < UDP_SINGLE_HEADER_LEN) {
                continue; // Too short
            }
            
            // Check magic and version
            ByteBuffer buf = ByteBuffer.wrap(received).order(ByteOrder.BIG_ENDIAN);
            short magic = buf.getShort();
            if (magic != UDP_MAGIC) {
                continue; // Wrong magic
            }
            
            byte version = buf.get();
            if (version != UDP_VERSION) {
                continue; // Wrong version
            }
            
            byte flags = buf.get();
            if (flags != UDP_FLAG_RESPONSE) {
                continue; // Not a response
            }
            
            int recvReqID = buf.getInt();
            if (recvReqID != (int) reqID) {
                continue; // Wrong request ID
            }
            
            // Check if single datagram or fragment
            if (received.length >= UDP_HEADER_LEN) {
                // Could be fragment - check if byte 8-9 look like seq_no
                short seqNo = buf.getShort();
                short fragCount = buf.getShort();
                
                if (fragCount > 0 && seqNo < fragCount && received.length >= UDP_HEADER_LEN) {
                    // It's a fragment
                    short payloadLen = buf.getShort();
                    if (payloadLen > 0 && received.length >= UDP_HEADER_LEN + payloadLen) {
                        byte[] payload = new byte[payloadLen];
                        buf.get(payload);
                        
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
                    continue;
                }
            }
            
            // Single datagram response
            if (received.length >= UDP_SINGLE_HEADER_LEN) {
                byte status = received[UDP_SINGLE_HEADER_LEN - 1];
                byte[] data = Arrays.copyOfRange(received, UDP_SINGLE_HEADER_LEN, received.length);
                
                // Prepend status byte for consistency
                byte[] response = new byte[1 + data.length];
                response[0] = status;
                System.arraycopy(data, 0, response, 1, data.length);
                
                return response;
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

