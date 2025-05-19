import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Main class for a simple Kafka clone that supports the ApiVersions and DescribeTopicPartitions requests.
 * <p>
 * The broker accepts connections on port 9092, reads a fixed 12-byte header
 * (4 bytes message size (size of payload), 2 bytes api_key, 2 bytes api_version, 4 bytes correlation_id),
 * and sends a response in Kafka’s flexible (compact) message format.
 * <p>
 * For ApiVersions requests (api_key == 18):
 * <ul>
 *   <li>If the requested api_version is unsupported (< 0 or > 4) an error response with error_code 35 is returned.</li>
 *   <li>For api_version 0–3, a successful response is returned with error_code 0 and two ApiVersion entries
 *       (api_key 18, min_version 0, max_version 4; api_key 75, min_version 0, max_version 0).</li>
 *   <li>For api_version 4, a minimal successful response is returned with error_code 0 and no ApiVersion entries.</li>
 * </ul>
 * <p>
 * For DescribeTopicPartitions requests (api_key == 75, version 0):
 * <ul>
 *   <li>Responds with error_code 3 (UNKNOWN_TOPIC_OR_PARTITION), echoing the requested topic name,
 *       a zeroed topic_id, empty partitions, and tester-specific fields.</li>
 * </ul>
 */
public class Main {
    private static final String METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    
    // Constants
    private static final byte[] TAG_BUFFER = new byte[]{0};
    private static final byte[] DEFAULT_THROTTLE_TIME = new byte[]{0, 0, 0, 0};
    private static final Map<String, byte[]> ERRORS = new HashMap<>();
    private static MetadataReader metadataReader;

    
    // Removed unused method

    /**
     * Handles client connections and processes requests.
     */
    private static void handleClient(Socket clientSocket) throws IOException {
        InputStream in = clientSocket.getInputStream();
        OutputStream out = clientSocket.getOutputStream();

        byte[] buffer = new byte[4096];
        int bytesRead;

        while ((bytesRead = in.read(buffer)) != -1) {
            try {
                // Parse the request header
                ByteBuffer headerBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
                headerBuffer.order(ByteOrder.BIG_ENDIAN);

                // Read message size (for logging only)
                int messageSize = headerBuffer.getInt();
                System.err.println("Message size: " + messageSize + " bytes");

                // Read API key
                short apiKey = headerBuffer.getShort();

                // Read API version
                short apiVersion = headerBuffer.getShort();

                // Read correlation ID
                int correlationId = headerBuffer.getInt();

                System.err.println("Received request: api_key=" + apiKey +
                                  ", api_version=" + apiVersion +
                                  ", correlation_id=" + correlationId);

                // Skip client ID (variable length)
                short clientIdLength = headerBuffer.getShort();
                if (clientIdLength > 0) {
                    byte[] clientIdBytes = new byte[clientIdLength];
                    headerBuffer.get(clientIdBytes);
                    String clientId = new String(clientIdBytes, StandardCharsets.UTF_8);
                    System.err.println("Client ID: " + clientId);
                }

                // Skip tagged fields
                if (headerBuffer.hasRemaining()) {
                    headerBuffer.get();
                }

                // Extract request body
                int bodyStart = headerBuffer.position();
                int bodyLength = bytesRead - bodyStart;
                byte[] body = new byte[bodyLength];
                if (bodyLength > 0) {
                    System.arraycopy(buffer, bodyStart, body, 0, bodyLength);

                    // Dump the full request for debugging
                    StringBuilder fullHexDump = new StringBuilder("Full request: ");
                    for (int i = 0; i < bytesRead; i++) {
                        fullHexDump.append(String.format("%02X ", buffer[i] & 0xFF));
                    }
                    System.err.println(fullHexDump.toString());
                }

                // Process based on API key
                byte[] response;
                if (apiKey == 18) { // ApiVersions
                    response = new ApiVersionsRequest(apiVersion, correlationId).buildResponse();
                } else if (apiKey == 75) { // DescribeTopicPartitions
                    response = new DescribeTopicPartitionsRequest(correlationId, body).buildResponse();
                } else {
                    System.err.println("Unknown API key: " + apiKey);
                    continue;
                }

                // Send response
                out.write(response);
                out.flush();
            } catch (Exception e) {
                System.err.println("Error handling request: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Class for handling ApiVersions requests.
     */
    static class ApiVersionsRequest {
        private final short apiVersion;
        private final int correlationId;
    
        public ApiVersionsRequest(short apiVersion, int correlationId) {
            this.apiVersion = apiVersion;
            this.correlationId = correlationId;
        }
    
        public byte[] buildResponse() {
            ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
    
            try {
                // Response header: correlation ID
                responseBody.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(correlationId).array());
    
                // Tagged fields in header
                responseBody.write(0); // Empty tagged fields
    
                // Error code
                boolean isSupported = apiVersion >= 0 && apiVersion <= 4;
                short errorCode = isSupported ? (short) 0 : (short) 35;
                responseBody.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort(errorCode).array());
    
                // API keys array
                if (errorCode == 0 && apiVersion < 4) { // Success case for versions 0–3
                    responseBody.write(3); // Compact array length (3-1=2 elements)
    
                    // ApiVersions (key 18)
                    responseBody.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 18).array());
                    responseBody.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                    responseBody.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 4).array());
                    responseBody.write(0); // Tagged fields
    
                    // DescribeTopicPartitions (key 75)
                    responseBody.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 75).array());
                    responseBody.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                    responseBody.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                    responseBody.write(0); // Tagged fields
                } else { // Error case (unsupported version) or version 4
                    responseBody.write(1); // Empty array (compact format: length 1 means 0 elements)
                }
    
                // Throttle time (ms)
                responseBody.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(0).array());
    
                // Tagged fields at end of response
                responseBody.write(0);
    
                // Create the final message with size prefix
                byte[] responseBytes = responseBody.toByteArray();
                ByteBuffer finalBuffer = ByteBuffer.allocate(4 + responseBytes.length);
                finalBuffer.order(ByteOrder.BIG_ENDIAN);
                finalBuffer.putInt(responseBytes.length);
                finalBuffer.put(responseBytes);
    
                byte[] result = finalBuffer.array();
                System.err.println("Built ApiVersions response (version " + apiVersion + "): " + bytesToHex(result));
                return result;
    
            } catch (IOException e) {
                System.err.println("Error building ApiVersions response: " + e.getMessage());
                return new byte[0];
            }
        }
    }

    /**
     * Runs the server on the specified port.
     * <p>
     * This method continuously accepts new client connections.
     * For each connection, a new thread is spawned to handle sequential requests from that client.
     * @param port the port to listen on.
     * @param metadata the metadata reader instance.
     * @throws IOException if an I/O error occurs.
     */
    public static void runServer(int port, MetadataReader metadata) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        System.err.println("Server is listening on port " + port);
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
                try {
                    handleClient(clientSocket);
                } catch (IOException e) {
                    System.err.println("Error handling client: " + e.getMessage());
                } finally {
                    try {
                        clientSocket.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }
            }
        } finally {
            serverSocket.close();
        }
    }

    /**
     * Main entry point.
     * @param args command-line arguments (optional port number).
     */
    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");

        try {
            // Initialize metadata reader
            metadataReader = new MetadataReader();

            // Start server
            runServer(9092, metadataReader);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
