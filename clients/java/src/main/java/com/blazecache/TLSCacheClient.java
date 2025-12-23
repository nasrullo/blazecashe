package com.blazecache;

import javax.net.ssl.*;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TLS-enabled cache client for BlazeCache.
 * Provides the same interface as CacheClient but uses TLS encryption for all connections.
 */
public class TLSCacheClient {
    
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
    private final SSLContext sslContext;
    private final boolean verifyCertificates;
    
    // Connection pooling (optimized for high throughput)
    private final ConcurrentHashMap<String, BlockingQueue<Socket>> connectionPools = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> poolCounts = new ConcurrentHashMap<>();
    private static final int MAX_POOL_SIZE = 500; // Maximum connections per server
    private static final int CONNECTION_TIMEOUT = 5000; // 5 seconds
    
    /**
     * Creates a new TLS client with certificate verification enabled.
     * 
     * @param servers List of server addresses in format "hostname:port"
     * @throws IOException if SSL context initialization fails
     */
    public TLSCacheClient(List<String> servers) throws IOException {
        this(servers, SelectionStrategy.ROUND_ROBIN, Collections.emptyList(), true);
    }
    
    /**
     * Creates a new TLS client with certificate verification enabled.
     * 
     * @param servers List of server addresses
     * @param strategy Server selection strategy
     * @throws IOException if SSL context initialization fails
     */
    public TLSCacheClient(List<String> servers, SelectionStrategy strategy) throws IOException {
        this(servers, strategy, Collections.emptyList(), true);
    }
    
    /**
     * Creates a new TLS client.
     * 
     * @param servers List of server addresses
     * @param strategy Server selection strategy
     * @param verifyCertificates If false, skips certificate verification (development only)
     * @throws IOException if SSL context initialization fails
     */
    public TLSCacheClient(List<String> servers, SelectionStrategy strategy, boolean verifyCertificates) throws IOException {
        this(servers, strategy, Collections.emptyList(), verifyCertificates);
    }
    
    private TLSCacheClient(List<String> servers, SelectionStrategy strategy, List<Integer> weights, boolean verifyCertificates) throws IOException {
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("At least one server required");
        }
        this.servers = new ArrayList<>(servers);
        this.strategy = strategy;
        this.weights = new ArrayList<>(weights);
        this.verifyCertificates = verifyCertificates;
        
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
        
        // Initialize SSL context
        try {
            this.sslContext = createSSLContext(verifyCertificates);
        } catch (Exception e) {
            throw new IOException("Failed to initialize SSL context", e);
        }
    }
    
    private SSLContext createSSLContext(boolean verifyCertificates) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        
        if (verifyCertificates) {
            // Use default trust managers (system certificates)
            context.init(null, null, null);
        } else {
            // Create trust manager that accepts all certificates (development only)
            TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() { return null; }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) { }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                }
            };
            context.init(null, trustAllCerts, null);
        }
        
        return context;
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
    
    // Connection pooling methods
    private Socket getOrCreateConnection(String server) throws IOException {
        BlockingQueue<Socket> pool = connectionPools.get(server);
        if (pool != null) {
            Socket conn = pool.poll();
            if (conn != null) {
                return conn;
            }
        }
        
        if (pool == null) {
            pool = new LinkedBlockingQueue<>(MAX_POOL_SIZE);
            BlockingQueue<Socket> existing = connectionPools.putIfAbsent(server, pool);
            if (existing != null) {
                pool = existing;
            }
            poolCounts.putIfAbsent(server, new AtomicInteger(0));
        }
        
        AtomicInteger count = poolCounts.get(server);
        int current = count.get();
        
        if (current < MAX_POOL_SIZE) {
            if (count.compareAndSet(current, current + 1)) {
                try {
                    Socket conn = createConnection(server);
                    Socket pooled = pool.poll();
                    if (pooled != null) {
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
        
        Socket pooled = pool.poll();
        if (pooled != null) {
            return pooled;
        }
        
        Socket conn = createConnection(server);
        count.incrementAndGet();
        return conn;
    }
    
    private final ConcurrentHashMap<String, java.net.InetSocketAddress> addressCache = new ConcurrentHashMap<>();
    
    private Socket createConnection(String server) throws IOException {
        java.net.InetSocketAddress addr = addressCache.computeIfAbsent(server, s -> {
            String[] parts = s.split(":", 2);
            return new java.net.InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
        });
        
        // Create SSL socket instead of plain socket
        SSLSocketFactory factory = sslContext.getSocketFactory();
        SSLSocket sslSocket = (SSLSocket) factory.createSocket(addr.getAddress(), addr.getPort());
        sslSocket.setSoTimeout(timeout);
        sslSocket.startHandshake(); // Perform TLS handshake
        
        return sslSocket;
    }
    
    private void returnConnection(String server, Socket socket) {
        BlockingQueue<Socket> pool = connectionPools.get(server);
        if (pool != null) {
            if (!pool.offer(socket)) {
                // Pool is full, close the connection
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
    
    private void markConnectionDead(String server) {
        AtomicInteger count = poolCounts.get(server);
        if (count != null) {
            count.decrementAndGet();
        }
    }
    
    // Protocol encoding/decoding (same as CacheClient)
    private byte[] encodeRequest(byte command, String key, byte[] value) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write(command);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            baos.write((byte) ((keyBytes.length >> 8) & 0xFF));
            baos.write((byte) (keyBytes.length & 0xFF));
            baos.write(keyBytes);
            if (command == 0x02) { // PUT
                baos.write((byte) ((value.length >> 24) & 0xFF));
                baos.write((byte) ((value.length >> 16) & 0xFF));
                baos.write((byte) ((value.length >> 8) & 0xFF));
                baos.write((byte) (value.length & 0xFF));
                baos.write(value);
                baos.write((byte) 0); // TTL (0 = no expiration)
                baos.write((byte) 0);
                baos.write((byte) 0);
                baos.write((byte) 0);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }
    
    private byte[] readResponse(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        // Read status byte first
        byte[] statusByte = new byte[1];
        int read = 0;
        while (read < 1) {
            int chunk = is.read(statusByte, read, 1 - read);
            if (chunk == -1) {
                throw new IOException("Connection closed while reading status");
            }
            read += chunk;
        }
        baos.write(statusByte);
        byte status = statusByte[0];
        
        if (status == 0x00) {
            // OK - read 4-byte data length + data
            byte[] lenBytes = new byte[4];
            read = 0;
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
            read = 0;
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
            // PONG - no additional data
        }
        
        return baos.toByteArray();
    }
    
    private static class ResponseData {
        byte status;
        String message;
        byte[] data;
    }
    
    private ResponseData decodeResponse(byte[] data) {
        ResponseData resp = new ResponseData();
        if (data.length < 1) {
            resp.status = 0x01;
            resp.message = "Response too short";
            return resp;
        }
        
        resp.status = data[0];
        int offset = 1;
        
        if (resp.status == 0x00) {
            // OK - read data length and data
            if (data.length < offset + 4) {
                resp.status = 0x01;
                resp.message = "Response too short for data length";
                return resp;
            }
            int dataLen = ((data[offset] & 0xFF) << 24) |
                         ((data[offset + 1] & 0xFF) << 16) |
                         ((data[offset + 2] & 0xFF) << 8) |
                         (data[offset + 3] & 0xFF);
            offset += 4;
            if (data.length < offset + dataLen) {
                resp.status = 0x01;
                resp.message = "Response too short for data";
                return resp;
            }
            resp.data = new byte[dataLen];
            System.arraycopy(data, offset, resp.data, 0, dataLen);
        } else if (resp.status == 0x01) {
            // ERROR - read message
            if (data.length < offset + 2) {
                resp.message = "";
                return resp;
            }
            int msgLen = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
            offset += 2;
            if (data.length >= offset + msgLen) {
                resp.message = new String(data, offset, msgLen, StandardCharsets.UTF_8);
            } else {
                resp.message = "";
            }
        }
        
        return resp;
    }
}

