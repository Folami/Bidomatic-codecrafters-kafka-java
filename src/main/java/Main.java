import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.UnsupportedEncodingException;

/**
 * Main class for a simple Kafka clone that supports the ApiVersions request.
 * <p>
 * The broker accepts connections on port 9092, reads a fixed 12-byte header 
 * (4 bytes message size (size of payload), 2 bytes api_key, 2 bytes api_version, 4 bytes correlation_id),
 * and sends a response in Kafka’s flexible (compact) message format.
 * <p>
 * For ApiVersions requests (api_key == 18):
 * <ul>
 *   <li>If the requested api_version is unsupported (< 0 or > 4) an error response with error_code 35 is returned.</li>
 *   <li>Otherwise, a successful response is returned with error_code 0 and one ApiVersion entry 
 *       (api_key 18, min_version 0, max_version 4).</li>
 * </ul>
 * <p>
 * The successful response body layout (15 bytes) is as follows:
 * <pre>
 *   error_code                : INT16 (2 bytes)
 *   api_keys (compact array)  : 1 byte (length = 2, i.e. one element + 1)
 *     - Entry:
 *         api_key           : INT16 (2 bytes)  (value 18)
 *         min_version       : INT16 (2 bytes)  (value 0)
 *         max_version       : INT16 (2 bytes)  (value 4)
 *         entry TAG_BUFFER  : 1 byte  (0x00 for empty)
 *   throttle_time_ms          : INT32 (4 bytes, value 0)
 *   response TAG_BUFFER       : 1 byte  (0x00 for empty)
 * </pre>
 * <p>
 * The overall message after the message_length field is:
 * 4 bytes (correlation_id) + 15 bytes (body) = 19 bytes.
 * Combined with the 4-byte message_length field, the total transmission is 23 bytes.
 */
public class Main {

    /**
     * Helper class to hold the incoming request header.
     */
    public static class RequestHeader {
        public int requestSize;
        public short apiKey;
        public short apiVersion;
        public int correlationId;

        public RequestHeader(int requestSize, short apiKey, short apiVersion, int correlationId) {
            this.requestSize = requestSize;
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
            this.correlationId = correlationId;
        }
    }

    /**
     * Reads exactly n bytes from the InputStream.
     * @param in the input stream.
     * @param n the number of bytes to read.
     * @return the read bytes.
     * @throws IOException if the connection is closed before n bytes are read.
     */
    public static byte[] readNBytes(InputStream in, int n) throws IOException {
        byte[] data = new byte[n];
        int totalRead = 0;
        while (totalRead < n) {
            int bytesRead = in.read(data, totalRead, n - totalRead);
            if (bytesRead == -1) {
                throw new IOException("Expected " + n + " bytes, got " + totalRead + " bytes");
            }
            totalRead += bytesRead;
        }
        return data;
    }

    /**
     * Reads the fixed 12-byte header from the socket.
     * <p>
     * Header layout:
     * <ul>
     *   <li>4 bytes: request message size (size of data following this field)</li>
     *   <li>2 bytes: api_key (INT16)</li>
     *   <li>2 bytes: api_version (INT16)</li>
     *   <li>4 bytes: correlation_id (INT32)</li>
     * </ul>
     * @param in the socket input stream.
     * @return the populated RequestHeader.
     * @throws IOException if reading fails.
     */
    public static RequestHeader readRequestHeader(InputStream in) throws IOException {
        // Read size field (4 bytes)
        byte[] sizeBytes = readNBytes(in, 4);
        ByteBuffer sizeBuffer = ByteBuffer.wrap(sizeBytes);
        sizeBuffer.order(ByteOrder.BIG_ENDIAN);
        int messageSize = sizeBuffer.getInt();

        // Read the rest of the header (8 bytes)
        byte[] headerBytes = readNBytes(in, 8);
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerBytes);
        headerBuffer.order(ByteOrder.BIG_ENDIAN);
        short apiKey = headerBuffer.getShort();
        short apiVersion = headerBuffer.getShort();
        int correlationId = headerBuffer.getInt();

        return new RequestHeader(messageSize, apiKey, apiVersion, correlationId);
    }

    /**
     * Discards remaining bytes from the request.
     * @param in the socket input stream.
     * @param remaining the number of remaining bytes to discard.
     */
    public static void discardRemainingRequest(InputStream in, int remaining) throws IOException {
        while (remaining > 0) {
            byte[] buffer = new byte[Math.min(4096, remaining)];
            int read = in.read(buffer);
            if (read == -1)
                break;
            remaining -= read;
        }
    }

    /**
     * Builds an ApiVersions response.
     * <p>
     * This method checks the requested api_version and builds the appropriate response.
     * If the requested version is unsupported, it sends an error response with error_code 35.
     * Otherwise, it sends a success response with error_code 0 and one ApiVersion entry.
     * @param header the request header containing api_key, api_version, and correlation_id.
     * @param out the output stream to send the response to.
     */
    public static void buildApiVersionsResponse(RequestHeader header, OutputStream out) throws IOException {
        if (header.apiVersion < 0 || header.apiVersion > 4) {
            System.err.println("Unsupported api_version " + header.apiVersion + ", sending error response.");
            byte[] errorResponse = buildErrorResponse(header.correlationId, (short) 35);
            out.write(errorResponse);
        } else {
            System.err.println("Sending success response for api_version " + header.apiVersion);
            byte[] successResponse = buildSuccessResponse(header.correlationId);
            out.write(successResponse);
        }
        out.flush();
    }

    /**
     * Builds an error response when the requested API version is unsupported.
     * <p>
     * Constructs a response that contains:
     * <ul>
     *   <li>error_code: INT16 (2 bytes)</li>
     *   <li>compact array: 1 byte (0, meaning no elements) </li>
     *   <li>throttle_time_ms: INT32 (4 bytes, value 0)</li>
     *   <li>response TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * Overall, the body is 8 bytes; prepended by correlation_id (4 bytes) gives 12.
     * The overall response includes a 4-byte message_length (which is 12).
     * @param correlationId the correlation id from the request.
     * @param errorCode the error code (e.g. 35).
     * @return the complete error response bytes.
     */
    public static byte[] buildErrorResponse(int correlationId, short errorCode) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(4 + 8); // message_length = 12 (correlation_id + body)
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.put((byte) 0); // compact array: no elements
        buffer.putInt(0);     // throttle_time_ms
        buffer.put((byte) 0); // TAG_BUFFER
        return buffer.array();
    }

    /**
     * Builds a successful API Versions response.
     * <p>
     * Constructs a response with:
     * <ul>
     *   <li>error_code: INT16 (2 bytes, value 0)</li>
     *   <li>api_keys: compact array length: 1 byte (value 2, meaning one element + 1)</li>
     *   <li>An ApiVersion entry (7 bytes) containing:
     *        api_key: INT16 (value 18),
     *        min_version: INT16 (value 0),
     *        max_version: INT16 (value 4),
     *        entry TAG_BUFFER: 1 byte (0x00)
     *   </li>
     *   <li>throttle_time_ms: INT32 (value 0)</li>
     *   <li>overall TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * Overall the body is 15 bytes; with correlation_id (4 bytes) it totals 19,
     * and with the message_length field (4 bytes) the complete response is 23 bytes.
     * @param correlationId the correlation id from the request.
     * @return the complete success response bytes.
     */
    public static byte[] buildSuccessResponse(int correlationId) {
        // Successful response body:
        // error_code         : 2 bytes
        // compact array length: 1 byte (3 = 2 entries + 1)
        // Entry 1 (ApiVersions): 7 bytes 
        //     - api_key: 2 bytes (18)
        //     - min_version: 2 bytes (0)
        //     - max_version: 2 bytes (4)
        //     - TAG_BUFFER: 1 byte
        // Entry 2 (DescribeTopicPartitions): 7 bytes
        //     - api_key: 2 bytes (75)
        //     - min_version: 2 bytes (0)
        //     - max_version: 2 bytes (0)
        //     - TAG_BUFFER: 1 byte
        // throttle_time_ms   : 4 bytes (0)
        // overall TAG_BUFFER : 1 byte (0)
        // Total body = 2 + 1 + 7 + 7 + 4 + 1 = 22 bytes.
        //
        // The full response consists of:
        // message_length (4 bytes) + correlation_id (4 bytes) + body (22 bytes) = 30 bytes.
        int bodySize = 22;
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + bodySize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(4 + bodySize);      // message_length = 4 (correlation_id) + bodySize = 26 bytes payload
        buffer.putInt(correlationId);       // correlation_id
        buffer.putShort((short) 0);         // error_code = 0
        buffer.put((byte) 3);               // compact array length = 3 (i.e. 2 entries + 1)
        
        // Entry 1: ApiVersions (api_key 18)
        buffer.putShort((short) 18);        // api_key = 18
        buffer.putShort((short) 0);         // min_version = 0
        buffer.putShort((short) 4);         // max_version = 4
        buffer.put((byte) 0);               // entry TAG_BUFFER
        
        // Entry 2: DescribeTopicPartitions (api_key 75)
        buffer.putShort((short) 75);        // api_key = 75
        buffer.putShort((short) 0);         // min_version = 0
        buffer.putShort((short) 0);         // max_version = 0
        buffer.put((byte) 0);               // entry TAG_BUFFER
        
        buffer.putInt(0);                   // throttle_time_ms = 0
        buffer.put((byte) 0);               // overall TAG_BUFFER = 0
        return buffer.array();
    }

    /**
     * Parses a DescribeTopicPartitions (v0) request body.
     * The request body format is:
     *  - topics: array of topics:
     *       int16 topicsCount
     *       For each topic:
     *          string topic_name (2-byte length + UTF-8 bytes)
     *          int32 partitionsCount
     *          For each partition: int32 partition id (ignored)
     * Returns the topic name of the first topic.
     */
    public static String parseDescribeTopicPartitionsRequest(InputStream in, int remaining) throws IOException {
        // Read the topic string length (2 bytes) and the topic name bytes.
        byte[] topicLengthBytes = readNBytes(in, 2);
        short topicLength = ByteBuffer.wrap(topicLengthBytes).order(ByteOrder.BIG_ENDIAN).getShort();
        byte[] topicNameBytes = readNBytes(in, topicLength);
        // Discard any extra bytes from the request body.
        int bytesRead = 2 + topicLength;
        if (remaining > bytesRead) {
            discardRemainingRequest(in, remaining - bytesRead);
        }
        return new String(topicNameBytes, "UTF-8");
    }

    /**
     * Builds a DescribeTopicPartitions (v0) response for an unknown topic.
     * The response body is:
     *   - error_code: INT16 (2 bytes) = 3 (UNKNOWN_TOPIC_OR_PARTITION)
     *   - topic_name: Kafka-encoded string (2-byte length + UTF-8 bytes)
     *   - topic_id: 16 bytes of zeros (UUID all zeros)
     *   - partitions: int32 count = 0 (empty array)
     * The full response: message_length (4 bytes) + correlation_id (4 bytes) + body.
     */
    public static byte[] buildDescribeTopicPartitionsResponse(int correlationId, String topic) throws UnsupportedEncodingException {
        // Convert topic to UTF-8 bytes
        byte[] topicBytes = topic.getBytes("UTF-8");
        
        // Create a fixed 115-byte field for the topic name, padded with zeros
        byte[] topicField = new byte[115];
        System.arraycopy(topicBytes, 0, topicField, 0, Math.min(topicBytes.length, 115));
        
        // Fixed topic_id (16 bytes of zeros)
        byte[] topicId = new byte[16];
        
        // Body size: error_code (2) + topic_field (115) + topic_id (16) + partitions_count (4)
        int bodySize = 2 + 115 + 16 + 4;
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + bodySize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        // message_length = correlation_id (4) + body size
        buffer.putInt(4 + bodySize);
        buffer.putInt(correlationId);
        buffer.putShort((short) 3);  // error_code = 3 (UNKNOWN_TOPIC_OR_PARTITION)
        buffer.put(topicField);      // fixed 115-byte topic field
        buffer.put(topicId);         // 16 zero bytes for topic_id
        buffer.putInt(0);            // empty partitions array (count = 0)
        
        return buffer.array();
    }

    /**
     * Modified handleClient to support both ApiVersions (api_key 18) and
     * DescribeTopicPartitions (api_key 75) requests.
     */
    public static void handleClient(Socket clientSocket) {
        try {
            InputStream clientInputStream = clientSocket.getInputStream();
            OutputStream clientOutputStream = clientSocket.getOutputStream();
            while (true) {
                RequestHeader header;
                try {
                    header = readRequestHeader(clientInputStream);
                } catch (IOException ioe) {
                    // End of stream or error reading header.
                    break;
                }
                System.err.println("Received correlation_id: " + header.correlationId
                        + ", requested api_version: " + header.apiVersion
                        + ", api_key: " + header.apiKey);
                System.err.println("Received request size: " + header.requestSize);

                int remainingBytes = header.requestSize - 8;
                if (header.apiKey == 18) {
                    if (remainingBytes > 0) {
                        discardRemainingRequest(clientInputStream, remainingBytes);
                        System.err.println("Discarded " + remainingBytes + " bytes from ApiVersions request.");
                    }
                    buildApiVersionsResponse(header, clientOutputStream);
                } else if (header.apiKey == 75) {
                    // For DescribeTopicPartitions (v0), parse the request to get the topic name.
                    String topic = parseDescribeTopicPartitionsRequest(clientInputStream, remainingBytes);
                    byte[] response = buildDescribeTopicPartitionsResponse(header.correlationId, topic);
                    clientOutputStream.write(response);
                    clientOutputStream.flush();
                    System.err.println("Sent DescribeTopicPartitions response (" + response.length + " bytes)");
                } else {
                    System.err.println("Unknown api_key " + header.apiKey + ", skipping.");
                    if (remainingBytes > 0) {
                        discardRemainingRequest(clientInputStream, remainingBytes);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.shutdownOutput();
            } catch (IOException e) { }
            try {
                clientSocket.close();
            } catch (IOException e) { }
            System.err.println("Client connection closed.");
        }
    }

    /**
     * Runs the server on the specified port.
     * <p>
     * This method continuously accepts new client connections.
     * For each connection, a new thread is spawned to handle sequential requests from that client.
     * @param port the port to listen on.
     * @throws IOException if an I/O error occurs.
     */
    public static void runServer(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        System.err.println("Server is listening on port " + port);
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
                // Spawn a new thread to handle this client concurrently.
                new Thread(() -> handleClient(clientSocket)).start();
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
        int port = 9092;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number, using default port 9092.");
            }
        }
        System.err.println("Starting server on port " + port);
        System.err.println("Logs from your program will appear here!");
        try {
            runServer(port);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
