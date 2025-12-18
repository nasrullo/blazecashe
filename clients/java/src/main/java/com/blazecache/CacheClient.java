package com.blazecache;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
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
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            
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
                    throw new IOException("Server error: " + resp.message);
            }
        }
    }
    
    public void set(String key, byte[] value) throws IOException {
        String server = selectServer(key);
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            
            byte[] request = encodeRequest((byte) 0x02, key, value);
            socket.getOutputStream().write(request);
            
            byte[] response = readResponse(socket.getInputStream());
            ResponseData resp = decodeResponse(response);
            
            if (resp.status != 0x00) {
                throw new IOException("Set failed: " + resp.message);
            }
        }
    }
    
    public boolean delete(String key) throws IOException {
        String server = selectServer(key);
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            
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
                    throw new IOException("Delete failed: " + resp.message);
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
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            
            // PING is just command byte 0x00 (no key/data)
            socket.getOutputStream().write(new byte[]{(byte) 0x00});
            
            byte[] response = readResponse(socket.getInputStream());
            ResponseData resp = decodeResponse(response);
            
            if (resp.status != 0x02) {
                throw new IOException("Ping failed: expected PONG (0x02), got " + resp.status);
            }
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
