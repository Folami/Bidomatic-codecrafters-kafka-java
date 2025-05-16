import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.net.*;

public class Main {
    private static ClusterMetadataReader metadataReader;
    
    public static void main(String[] args) {
        int port = 9092;
        
        // Initialize the metadata reader
        metadataReader = new ClusterMetadataReader();
        
        System.err.println("Starting server on port " + port);
        System.err.println("Logs from your program will appear here!");
        
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.err.println("Server is listening on port " + port);
            
            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
                    handleClient(clientSocket);
                } catch (IOException e) {
                    System.err.println("Error handling client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }
    
    static void handleClient(Socket clientSocket) throws IOException {
        InputStream in = clientSocket.getInputStream();
        OutputStream out = clientSocket.getOutputStream();
        
        try {
            while (true) {
                // Read message size
                byte[] sizeBytes = readNBytes(in, 4);
                if (sizeBytes.length < 4) {
                    throw new IOException("Expected 4 bytes, got " + sizeBytes.length + " bytes");
                }
                
                int messageSize = ByteBuffer.wrap(sizeBytes).getInt();
                
                // Read API key and version
                byte[] headerBytes = readNBytes(in, 8);
                if (headerBytes.length < 8) {
                    throw new IOException("Expected 8 bytes, got " + headerBytes.length + " bytes");
                }
                
                ByteBuffer headerBuffer = ByteBuffer.wrap(headerBytes);
                short apiKey = headerBuffer.getShort();
                short apiVersion = headerBuffer.getShort();
                int correlationId = headerBuffer.getInt();
                
                // Read client ID
                byte[] clientIdLenBytes = readNBytes(in, 2);
                if (clientIdLenBytes.length < 2) {
                    throw new IOException("Expected 2 bytes, got " + clientIdLenBytes.length + " bytes");
                }
                
                short clientIdLen = ByteBuffer.wrap(clientIdLenBytes).getShort();
                String clientId = "";
                
                if (clientIdLen > 0) {
                    byte[] clientIdBytes = readNBytes(in, clientIdLen);
                    if (clientIdBytes.length < clientIdLen) {
                        throw new IOException("Expected " + clientIdLen + " bytes, got " + clientIdBytes.length + " bytes");
                    }
                    clientId = new String(clientIdBytes, "UTF-8");
                }
                
                // Calculate body size
                int headerSize = 4 + 8 + 2 + clientIdLen;
                int bodySize = messageSize - (headerSize - 4);
                
                System.err.println("Received correlation_id: " + correlationId + 
                                  ", requested api_version: " + apiVersion + 
                                  ", api_key: " + apiKey + 
                                  ", client_id: '" + clientId + "'" + 
                                  ", client_id_length: " + (clientIdLen + 2) + 
                                  ", body_size: " + bodySize);
                
                // Handle different API requests
                if (apiKey == 18) { // ApiVersions
                    handleApiVersionsRequest(out, correlationId, apiVersion, bodySize, in);
                } else if (apiKey == 75) { // DescribeTopicPartitions
                    handleDescribeTopicPartitionsRequest(out, correlationId, apiVersion, bodySize, in);
                } else {
                    // Skip unknown request body
                    if (bodySize > 0) {
                        in.skip(bodySize);
                    }
                    System.err.println("Unsupported API key: " + apiKey);
                }
            }
        } catch (IOException e) {
            System.err.println("IOException while reading header, closing connection: " + e.getMessage());
        } finally {
            System.err.println("Client connection closed.");
        }
    }
    
    static void handleApiVersionsRequest(OutputStream out, int correlationId, short apiVersion, int bodySize, InputStream in) throws IOException {
        // Skip request body
        if (bodySize > 0) {
            in.skip(bodySize);
            System.err.println("Discarded " + bodySize + " bytes from ApiVersions request body.");
        }
        
        // Build and send response
        byte[] response = buildApiVersionsResponse(correlationId, apiVersion);
        out.write(response);
        out.flush();
        
        System.err.println("Sent ApiVersions response (" + response.length + " bytes)");
    }
    
    static void handleDescribeTopicPartitionsRequest(OutputStream out, int correlationId, short apiVersion, int bodySize, InputStream in) throws IOException {
        // Parse the request to get the topic name
        // Skip tagged fields
        int tagged = in.read();
        System.err.println("parse_tagged_field: tagged=" + tagged);
        
        // Read topics array length (compact format)
        int arrayLength = in.read();
        System.err.println("P_DTP_R: array_length=" + arrayLength);
        
        // Read topic name length (compact format)
        int topicNameLength = in.read();
        System.err.println("P_DTP_R: topic_name_length=" + topicNameLength);
        
        // Read topic name
        byte[] topicNameBytes = readNBytes(in, topicNameLength - 1); // Subtract 1 for compact format
        String topicName = new String(topicNameBytes, "UTF-8");
        System.err.println("P_DTP_R: topic_name='" + topicName + "'");
        
        // Read cursor
        int cursor = in.read();
        System.err.println("P_DTP_R: cursor=" + cursor);
        
        // Skip the rest of the request
        int remaining = bodySize - (1 + 1 + 1 + (topicNameLength - 1) + 1);
        if (remaining > 0) {
            in.skip(remaining);
            System.err.println("P_DTP_R: Discarding " + remaining + " unparsed bytes.");
        }
        
        System.err.println("Parsed DescribeTopicPartitions v0 request for topic: '" + topicName + "'");
        System.err.println("build_describe_topic_partitions_response: topic_name='" + topicName + 
                          "', array_length=" + arrayLength + 
                          ", topic_name_length=" + topicNameLength + 
                          ", cursor=" + cursor);
        
        // Build and send response
        byte[] response = buildDescribeTopicPartitionsResponse(correlationId, topicName);
        out.write(response);
        out.flush();
        
        System.err.println("Sent DescribeTopicPartitions v0 response (" + response.length + " bytes)");
    }
    
    static byte[] buildApiVersionsResponse(int correlationId, short apiVersion) {
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        
        try {
            // Check if the requested API version is supported
            boolean isSupported = apiVersion >= 0 && apiVersion <= 4;
            short errorCode = isSupported ? (short) 0 : (short) 35;
            
            // Calculate total size (will be filled in later)
            response.write(new byte[4]);
            
            // Response header: correlation ID
            response.write(ByteBuffer.allocate(4).putInt(correlationId).array());
            
            // Header tagged fields (empty)
            response.write(0);
            
            // Error code
            response.write(ByteBuffer.allocate(2).putShort(errorCode).array());
            
            if (isSupported) {
                // API keys array - compact array format
                // Value 3 means 2 elements (3-1=2)
                response.write(3);
                
                // First API key entry: ApiVersions (key 18)
                response.write(ByteBuffer.allocate(2).putShort((short) 18).array());
                response.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
                response.write(ByteBuffer.allocate(2).putShort((short) 4).array());  // max_version
                response.write(0);  // Tagged fields (empty)
                
                // Second API key entry: DescribeTopicPartitions (key 75)
                response.write(ByteBuffer.allocate(2).putShort((short) 75).array());
                response.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
                response.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // max_version
                response.write(0);  // Tagged fields (empty)
            } else {
                // Empty API keys array for error case
                response.write(1);  // Value 1 means 0 elements (1-1=0)
            }
            
            // Throttle time (ms)
            response.write(ByteBuffer.allocate(4).putInt(0).array());
            
            // Tagged fields (empty)
            response.write(0);
            
            // Fill in the total size at the beginning
            byte[] responseBytes = response.toByteArray();
            ByteBuffer.wrap(responseBytes, 0, 4).putInt(responseBytes.length - 4);
            
            return responseBytes;
        } catch (IOException e) {
            System.err.println("Error building ApiVersions response: " + e.getMessage());
            return new byte[0];
        }
    }
    
    static byte[] buildDescribeTopicPartitionsResponse(int correlationId, String topicName) {
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        
        try {
            // Check if the topic exists
            TopicMetadata topicMetadata = metadataReader.getTopicMetadata(topicName);
            boolean topicExists = (topicMetadata != null);
            
            System.err.println("Topic '" + topicName
                                  + "' exists: " + topicExists);
            
            if (topicExists) {
                // Build response for existing topic
                // (You can add the actual implementation here)
                // For now, we'll send a placeholder response
                response.write(buildExistingTopicResponse(correlationId, topicName));
            } else {
                // Build response for unknown topic
                response.write(buildUnknownTopicResponse(correlationId, topicName));
            }
            
            return response.toByteArray();
        } catch (IOException e) {
            System.err.println("Error building DescribeTopicPartitions response: " + e.getMessage());
            return new byte[0];
        }
    }
    
    static byte[] buildExistingTopicResponse(int correlationId, String topicName) {
        // Implement the response for an existing topic
        // This is a placeholder implementation
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        
        try {
            // Add the actual implementation here
            // For now, we'll send a placeholder response
            response.write(ByteBuffer.allocate(4).putInt(correlationId).array());
            response.write(ByteBuffer.allocate(4).putInt(0).array()); // throttle_time_ms
            response.write(0); // tagged fields (empty)
            response.write(1); // topics array length (compact format)
            response.write(1); // topic name length (compact format)
            response.write(topicName.getBytes("UTF-8"));
            response.write(0); // topic_id (16 bytes of zeros)
            response.write(0); // is_internal (BOOLEAN)
            response.write(1); // partition_array length (compact format)
            response.write(0); // partition_array (empty)
            response.write(ByteBuffer.allocate(4).putInt(0x00000df8).array()); // topic_authorized_operations
            response.write(0); // topic_tag_buffer (empty)
            response.write(0); // cursor
            response.write(0); // response_tag_buffer (empty)
            
            return response.toByteArray();
        } catch (IOException e) {
            System.err.println("Error building existing topic response: " + e.getMessage());
            return new byte[0];
        }
    }
    
    static byte[] buildUnknownTopicResponse(int correlationId, String topicName) {
        // Implement the response for an unknown topic
        // This is a placeholder implementation
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        
        try {
            // Add the actual implementation here
            // For now, we'll send a placeholder response
            response.write(ByteBuffer.allocate(4).putInt(correlationId).array());
            response.write(ByteBuffer.allocate(4).putInt(0).array()); // throttle_time_ms
            response.write(0); // tagged fields (empty)
            response.write(1); // topics array length (compact format)
            response.write(1); // topic name length (compact format)
            response.write(topicName.getBytes("UTF-8"));
            response.write(0); // topic_id (16 bytes of zeros)
            response.write(0); // is_internal (BOOLEAN)
            response.write(1); // partition_array length (compact format)
            response.write(0); // partition_array (empty)
            response.write(ByteBuffer.allocate(4).putInt(0x00000df8).array()); // topic_authorized_operations
            response.write(0); // topic_tag_buffer (empty)
            response.write(0); // cursor
            response.write(0); // response_tag_buffer (empty)
            
            return response.toByteArray();
        } catch (IOException e) {
            System.err.println("Error building unknown topic response: " + e.getMessage());
            return new byte[0];
        }
    }
}
