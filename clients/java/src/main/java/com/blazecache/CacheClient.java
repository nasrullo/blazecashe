package com.blazecache;

import java.io.*;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CacheClient {
    
    public enum SelectionStrategy {
        ROUND_ROBIN,
        WEIGHTED_ROUND_ROBIN,
        CONSISTENT_HASHING
    }
    
    private final List<String> servers;
    private final SelectionStrategy strategy;
    private final List<Integer> weights;
    private final AtomicLong counter = new AtomicLong(0);
    private final int timeout = 5000; // 5 seconds
    private final List<String> expanded; // pre-expanded list for weighted round robin
    
    // Connection pooling (optimized for high throughput)
    private final ConcurrentHashMap<String, BlockingQueue<Socket>> connectionPools = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> poolCounts = new ConcurrentHashMap<>();
    private static final int MAX_POOL_SIZE = 500; // Maximum connections per server
    private static final int CONNECTION_TIMEOUT = 5000; // 5 seconds
    
    public CacheClient(List<String> servers) {
        this(servers, SelectionStrategy.ROUND_ROBIN, Collections.emptyList());
    }
    
    public CacheClient(List<String> servers, SelectionStrategy strategy) {
        this(servers, strategy, Collections.emptyList());
    }
    
    public CacheClient(List<String> servers, SelectionStrategy strategy, List<Integer> weights) {
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("At least one server required");
        }
        this.servers = new ArrayList<>(servers);
        this.strategy = strategy;
        this.weights = new ArrayList<>(weights);
        // Build expanded list for weighted round robin
        if (strategy == SelectionStrategy.WEIGHTED_ROUND_ROBIN && weights != null && weights.size() == servers.size()) {
            List<String> tmp = new ArrayList<>();
            for (int i = 0; i < servers.size(); i++) {
                int w = weights.get(i);
                for (int j = 0; j < w; j++) {
                    tmp.add(servers.get(i));
                }
            }
            this.expanded = tmp;
        } else {
            this.expanded = Collections.emptyList();
        }
    }
    
    private String selectServer(String key) {
        switch (strategy) {
            case ROUND_ROBIN:
                int index = (int) (counter.getAndIncrement() % servers.size());
                return servers.get(index);
            case WEIGHTED_ROUND_ROBIN:
                if (!expanded.isEmpty()) {
                    index = (int) (counter.getAndIncrement() % expanded.size());
                    return expanded.get(index);
                }
                index = (int) (counter.getAndIncrement() % servers.size());
                return servers.get(index);
            case CONSISTENT_HASHING:
                int hash = Math.abs(key.hashCode());
                index = hash % servers.size();
                return servers.get(index);
            default:
                return servers.get(0);
        }
    }
    
    public Optional<byte[]> get(String key) throws IOException {
        String server = selectServer(key);
        Socket socket = getOrCreateConnection(server);
        boolean shouldReturn = true;
        
        try {
            byte[] request = encodeRequest((byte) 0x01, key, new byte[0]);
            socket.getOutputStream().write(request);
            
            byte[] response = readResponse(socket.getInputStream());
            ResponseData resp = decodeResponse(response);
            
            switch (resp.status) {
                case 0x00:
                    return Optional.of(resp.data);
                case 0x01:
                    return Optional.empty();
                default:
                    shouldReturn = false;
                    markConnectionDead(server);
                    throw new IOException("Server error: " + resp.message);
            }
        } catch (IOException e) {
            shouldReturn = false;
            markConnectionDead(server);
            throw e;
        } finally {
            if (shouldReturn) {
                returnConnection(server, socket);
            } else if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
    
    public void set(String key, byte[] value) throws IOException {
        String server = selectServer(key);
        Socket socket = getOrCreateConnection(server);
        boolean shouldReturn = true;
        
        try {
            byte[] request = encodeRequest((byte) 0x02, key, value);
            socket.getOutputStream().write(request);
            
            byte[] response = readResponse(socket.getInputStream());
            ResponseData resp = decodeResponse(response);
            
            if (resp.status != 0x00) {
                shouldReturn = false;
                markConnectionDead(server);
                throw new IOException("Set failed: " + resp.message);
            }
        } catch (IOException e) {
            shouldReturn = false;
            markConnectionDead(server);
            throw e;
        } finally {
            if (shouldReturn) {
                returnConnection(server, socket);
            } else if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
    
    public boolean delete(String key) throws IOException {
        String server = selectServer(key);
        Socket socket = getOrCreateConnection(server);
        boolean shouldReturn = true;
        
        try {
            byte[] request = encodeRequest((byte) 0x03, key, new byte[0]);
            socket.getOutputStream().write(request);
            
            byte[] response = readResponse(socket.getInputStream());
            ResponseData resp = decodeResponse(response);
            
            switch (resp.status) {
                case 0x00:
                    return true;
                case 0x01:
                    return false;
                default:
                    shouldReturn = false;
                    markConnectionDead(server);
                    throw new IOException("Delete failed: " + resp.message);
            }
        } catch (IOException e) {
            shouldReturn = false;
            markConnectionDead(server);
            throw e;
        } finally {
            if (shouldReturn) {
                returnConnection(server, socket);
            } else if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
    
    public Map<String, byte[]> getMulti(List<String> keys) throws IOException {
        Map<String, byte[]> results = new HashMap<>();
        
        for (String key : keys) {
            Optional<byte[]> value = get(key);
            value.ifPresent(bytes -> results.put(key, bytes));
        }
        
        return results;
    }
    
    public void ping() throws IOException {
        if (servers.isEmpty()) {
            throw new IOException("No servers configured");
        }
        
        String server = servers.get(0);
        Socket socket = getOrCreateConnection(server);
        boolean shouldReturn = true;
        
        try {
            // PING is just command byte 0x00 (no key/data)
            socket.getOutputStream().write(new byte[]{(byte) 0x00});
            
            byte[] response = readResponse(socket.getInputStream());
            ResponseData resp = decodeResponse(response);
            
            if (resp.status != 0x02) {
                shouldReturn = false;
                markConnectionDead(server);
                throw new IOException("Ping failed: expected PONG (0x02), got " + resp.status);
            }
        } catch (IOException e) {
            shouldReturn = false;
            markConnectionDead(server);
            throw e;
        } finally {
            if (shouldReturn) {
                returnConnection(server, socket);
            } else if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
    
    // Connection pooling methods (optimized for high throughput)
    private Socket getOrCreateConnection(String server) throws IOException {
        // Fast path: try to get connection from pool (non-blocking)
        BlockingQueue<Socket> pool = connectionPools.get(server);
        if (pool != null) {
            Socket conn = pool.poll(); // Non-blocking
            if (conn != null) {
                // Verify connection is still alive
                if (conn.isConnected() && !conn.isClosed()) {
                    return conn;
                }
            }
        }
        
        // Pool doesn't exist or is empty - initialize or create new connection
        if (pool == null) {
            // Initialize pool for this server (only happens once per server)
            pool = new LinkedBlockingQueue<>(MAX_POOL_SIZE);
            BlockingQueue<Socket> existing = connectionPools.putIfAbsent(server, pool);
            if (existing != null) {
                pool = existing; // Another thread created it first
            }
            poolCounts.putIfAbsent(server, new AtomicInteger(0));
        }
        
        AtomicInteger count = poolCounts.get(server);
        int current = count.get();
        
        // Try to create new connection if pool not at max size
        if (current < MAX_POOL_SIZE) {
            if (count.compareAndSet(current, current + 1)) {
                // Successfully claimed slot, create connection
                try {
                    Socket conn = createConnection(server);
                    // Try pool one more time before returning new
                    Socket pooled = pool.poll();
                    if (pooled != null && pooled.isConnected() && !pooled.isClosed()) {
                        // Got one from pool, close new one and return pooled
                        count.decrementAndGet();
                        try {
                            conn.close();
                        } catch (IOException ignored) {}
                        return pooled;
                    }
                    return conn;
                } catch (IOException e) {
                    count.decrementAndGet();
                    throw e;
                }
            }
        }
        
        // CAS failed or pool at max size - try pool again or create new (allow overflow)
        Socket pooled = pool.poll();
        if (pooled != null && pooled.isConnected() && !pooled.isClosed()) {
            return pooled;
        }
        
        // Still nothing, create new connection (allow overflow to prevent blocking)
        Socket conn = createConnection(server);
        count.incrementAndGet();
        return conn;
    }
    
    private Socket createConnection(String server) throws IOException {
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        Socket socket = new Socket();
        socket.setSoTimeout(timeout);
        socket.setTcpNoDelay(true); // Disable Nagle's algorithm for low latency
        socket.connect(new java.net.InetSocketAddress(host, port), CONNECTION_TIMEOUT);
        
        return socket;
    }
    
    private void returnConnection(String server, Socket socket) {
        if (socket == null || socket.isClosed()) {
            return;
        }
        
        BlockingQueue<Socket> pool = connectionPools.get(server);
        if (pool == null) {
            // Pool doesn't exist, just close the connection
            try {
                socket.close();
            } catch (IOException ignored) {}
            return;
        }
        
        // Try non-blocking offer first (fast path)
        if (pool.offer(socket)) {
            // Successfully returned to pool
            return;
        }
        
        // Pool is full, close connection
        try {
            socket.close();
        } catch (IOException ignored) {}
        
        // Decrement counter atomically
        AtomicInteger count = poolCounts.get(server);
        if (count != null) {
            count.decrementAndGet();
        }
    }
    
    private void markConnectionDead(String server) {
        AtomicInteger count = poolCounts.get(server);
        if (count != null) {
            count.decrementAndGet();
        }
    }
    
    private byte[] encodeRequest(byte command, String key, byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeByte(command);
        
        if (command != 0x00) { // PING doesn't have key/data
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            dos.writeShort(keyBytes.length);
            dos.write(keyBytes);
            
            if (command == 0x02) { // PUT has value and TTL
                dos.writeInt(data.length);
                dos.write(data);
                dos.writeInt(0); // TTL = 0 (no expiration)
            }
        }
        
        return baos.toByteArray();
    }
    
    private byte[] readResponse(InputStream is) throws IOException {
        // Read status byte first
        int status = is.read();
        if (status == -1) {
            throw new IOException("Connection closed");
        }
        
        // Read the rest of the response based on status
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(status);
        
        if (status == 0x00) {
            // OK - read 4-byte data length + data
            byte[] lenBytes = new byte[4];
            int read = 0;
            while (read < 4) {
                int chunk = is.read(lenBytes, read, 4 - read);
                if (chunk == -1) {
                    throw new IOException("Connection closed while reading data length");
                }
                read += chunk;
            }
            baos.write(lenBytes);
            int dataLen = ((lenBytes[0] & 0xFF) << 24) | ((lenBytes[1] & 0xFF) << 16) | 
                         ((lenBytes[2] & 0xFF) << 8) | (lenBytes[3] & 0xFF);
            if (dataLen > 0) {
                byte[] data = new byte[dataLen];
                read = 0;
                while (read < dataLen) {
                    int chunk = is.read(data, read, dataLen - read);
                    if (chunk == -1) {
                        throw new IOException("Connection closed while reading data");
                    }
                    read += chunk;
                }
                baos.write(data);
            }
        } else if (status == 0x01) {
            // ERROR - read 2-byte message length + message
            byte[] lenBytes = new byte[2];
            int read = 0;
            while (read < 2) {
                int chunk = is.read(lenBytes, read, 2 - read);
                if (chunk == -1) {
                    throw new IOException("Connection closed while reading message length");
                }
                read += chunk;
            }
            baos.write(lenBytes);
            int msgLen = ((lenBytes[0] & 0xFF) << 8) | (lenBytes[1] & 0xFF);
            if (msgLen > 0) {
                byte[] msg = new byte[msgLen];
                read = 0;
                while (read < msgLen) {
                    int chunk = is.read(msg, read, msgLen - read);
                    if (chunk == -1) {
                        throw new IOException("Connection closed while reading message");
                    }
                    read += chunk;
                }
                baos.write(msg);
            }
        } else if (status == 0x02) {
            // PONG - just status byte, no additional data
        } else {
            throw new IOException("Unknown response status: " + status);
        }
        
        return baos.toByteArray();
    }
    
    private ResponseData decodeResponse(byte[] data) throws IOException {
        if (data.length == 0) {
            throw new IOException("Empty response");
        }
        
        byte status = data[0];
        
        if (status == 0x02) {
            // PONG - just status byte
            return new ResponseData(status, "", new byte[0]);
        }
        
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data, 1, data.length - 1));
        
        if (status == 0x00) {
            // OK - read data length and data
            int dataLen = dis.readInt();
            byte[] responseData = new byte[dataLen];
            if (dataLen > 0) {
                dis.readFully(responseData);
            }
            return new ResponseData(status, "", responseData);
        } else if (status == 0x01) {
            // ERROR - read message length and message
            short msgLen = dis.readShort();
            byte[] msgBytes = new byte[msgLen];
            if (msgLen > 0) {
                dis.readFully(msgBytes);
            }
            String message = new String(msgBytes, StandardCharsets.UTF_8);
            return new ResponseData(status, message, new byte[0]);
        } else {
            throw new IOException("Unknown response status: " + status);
        }
    }
    
    private static class ResponseData {
        final byte status;
        final String message;
        final byte[] data;
        
        ResponseData(byte status, String message, byte[] data) {
            this.status = status;
            this.message = message;
            this.data = data;
        }
    }
}
