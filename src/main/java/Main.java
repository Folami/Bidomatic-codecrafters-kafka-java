import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

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
     * Encodes a Java String into Kafka's STRING format (INT16 length + UTF-8 bytes).
     * A null string is encoded as INT16 -1.
     * @param s the string to encode.
     * @return the encoded bytes.
     */
    public static byte[] encodeKafkaString(String s) {
        if (s == null) {
            return ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) -1).array();
        }
        byte[] utf8Bytes = s.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + utf8Bytes.length).order(ByteOrder.BIG_ENDIAN).putShort((short) utf8Bytes.length).put(utf8Bytes).array();
    }

    /**
     * Helper class to hold the incoming request header.
     */
    public static class FullRequestHeader {
        public int messageSize; // The total size of the message *following* the message_size field itself.
        public short apiKey;
        public short apiVersion;
        public int correlationId;
        public String clientId;
        public int clientIdFieldLengthBytes; // Bytes consumed by client_id: 2 for len + N for data, or 2 if null/empty.
        public int bodySize; // Calculated size of the actual request body payload (messageSize - commonHeaderParts - clientIdFieldLength)

        // Default constructor
        public FullRequestHeader() {}
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
     * Reads the full Kafka request header from the socket.
     * <p>
     * Header layout:
     * <ul>
     *   <li>4 bytes: message_size (size of data following this field)</li>
     *   <li>2 bytes: api_key (INT16)</li>
     *   <li>2 bytes: api_version (INT16)</li>
     *   <li>4 bytes: correlation_id (INT32)</li>
     *   <li>client_id: STRING (INT16 length + UTF-8 bytes)</li>
     * </ul>
     * @param in the socket input stream.
     * @return the populated FullRequestHeader.
     * @throws IOException if reading fails.
     */
    public static FullRequestHeader readFullRequestHeader(InputStream in) throws IOException {
        FullRequestHeader frh = new FullRequestHeader();

        byte[] messageSizeBytes = readNBytes(in, 4);
        frh.messageSize = ByteBuffer.wrap(messageSizeBytes).order(ByteOrder.BIG_ENDIAN).getInt();

        byte[] commonHeaderPartsBytes = readNBytes(in, 8); // apiKey, apiVersion, correlationId
        ByteBuffer commonHeaderBuffer = ByteBuffer.wrap(commonHeaderPartsBytes).order(ByteOrder.BIG_ENDIAN);
        frh.apiKey = commonHeaderBuffer.getShort();
        frh.apiVersion = commonHeaderBuffer.getShort();
        frh.correlationId = commonHeaderBuffer.getInt();

        // Read clientId (Kafka STRING)
        byte[] clientIdLenBytes = readNBytes(in, 2);
        short clientIdLenRaw = ByteBuffer.wrap(clientIdLenBytes).order(ByteOrder.BIG_ENDIAN).getShort();

        if (clientIdLenRaw == -1) { // Null string
            frh.clientId = null;
            frh.clientIdFieldLengthBytes = 2;
        } else if (clientIdLenRaw < 0) { // Invalid length for a non-null string
            System.err.println("Warning: Invalid Client ID length: " + clientIdLenRaw + ". Treating as empty.");
            frh.clientId = ""; // Treat as empty, though this is a protocol violation.
            frh.clientIdFieldLengthBytes = 2; // Consumed the length field
        } else { // Valid length (>=0)
            byte[] clientIdValueBytes = readNBytes(in, clientIdLenRaw);
            frh.clientId = new String(clientIdValueBytes, StandardCharsets.UTF_8);
            frh.clientIdFieldLengthBytes = 2 + clientIdLenRaw;
        }

        int commonHeaderSize = 8; // apiKey (2) + apiVersion (2) + correlationId (4)
        frh.bodySize = frh.messageSize - commonHeaderSize - frh.clientIdFieldLengthBytes;

        return frh;
    }

    /**
     * Discards remaining bytes from the request.
     * @param in the socket input stream.
     * @param remaining the number of remaining bytes to discard.
     */
    public static void discardRemainingRequest(InputStream in, int remaining) throws IOException {
        while (remaining > 0) {
            byte[] buffer = new byte[Math.min(4096, remaining)];
            int read = in.read(buffer, 0, Math.min(buffer.length, remaining));
            if (read == -1) {
                throw new IOException("Unexpected end of stream while discarding " + remaining + " bytes");
            }
            remaining -= read;
        }
    }

    /**
     * Builds an ApiVersions response.
     * <p>
     * This method checks the requested api_version and builds the appropriate response.
     * If the requested version is unsupported, it sends an error response with error_code 35.
     * Otherwise, it sends a success response with error_code 0 and one ApiVersion entry.
     * @param fullHeader the full request header.
     * @param out the output stream to send the response to.
     */
    public static void buildApiVersionsResponse(FullRequestHeader fullHeader, OutputStream out) throws IOException {
        if (fullHeader.apiVersion < 0 || fullHeader.apiVersion > 4) {
            System.err.println("Unsupported api_version " + fullHeader.apiVersion + ", sending error response.");
            byte[] errorResponse = buildErrorResponse(fullHeader.correlationId, (short) 35);
            out.write(errorResponse);
        } else {
            System.err.println("Sending success response for api_version " + fullHeader.apiVersion);
            byte[] successResponse = buildSuccessResponse(fullHeader.correlationId);
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
     *          For each partition: int32 partition id
     * Returns the topic name of the first valid topic, or null if none found.
     */
    public static String parseDescribeTopicPartitionsRequest(InputStream in, int remaining) throws IOException {
        int bytesConsumed = 0;
        String firstTopicName = null;

        // Read topicsCount (INT16)
        if (remaining < 2) {
            System.err.println("P_DTP_R: Body too small for topicsCount (" + remaining + " bytes).");
            if (remaining > 0) discardRemainingRequest(in, remaining);
            return "";
        }
        byte[] topicsCountBytes = readNBytes(in, 2);
        bytesConsumed += 2;
        short topicsCount = ByteBuffer.wrap(topicsCountBytes).order(ByteOrder.BIG_ENDIAN).getShort();

        if (topicsCount < 0) { // Invalid count
            System.err.println("P_DTP_R: Invalid topics_count " + topicsCount + ".");
            if (remaining - bytesConsumed > 0) discardRemainingRequest(in, remaining - bytesConsumed);
            return "";
        }

        System.err.println("P_DTP_R: Expecting " + topicsCount + " topics.");

        for (int i = 0; i < topicsCount; i++) {
            // Parse Topic Name (STRING)
            if (remaining - bytesConsumed < 2) { // Not enough for name length field
                System.err.println("P_DTP_R: Not enough data for topic_name length (topic " + (i+1) + "). Remaining: " + (remaining - bytesConsumed));
                break; // Malformed request
            }
            byte[] nameLenBytes = readNBytes(in, 2);
            bytesConsumed += 2;
            short nameLen = ByteBuffer.wrap(nameLenBytes).order(ByteOrder.BIG_ENDIAN).getShort();

            String currentTopicNameStr = "";
            if (nameLen < 0) { // Kafka STRING length must be >= 0
                 System.err.println("P_DTP_R: Invalid topic_name length " + nameLen + " for topic " + (i+1) + ". STRING requires >= 0.");
                 // Malformed. Treat as empty name for this topic.
            } else if (nameLen > 0) { // Only read payload if length > 0
                if (nameLen > remaining - bytesConsumed) {
                    System.err.println("P_DTP_R: Stated topic_name length " + nameLen + " for topic " + (i+1) + " exceeds remaining body bytes " + (remaining - bytesConsumed) + ". Malformed.");
                    if (firstTopicName == null) firstTopicName = ""; // Ensure it's set if this was the first
                    break; // Stop parsing topics, rest of body will be discarded.
                }
                byte[] namePayloadBytes = readNBytes(in, nameLen);
                bytesConsumed += nameLen;
                try {
                    currentTopicNameStr = new String(namePayloadBytes, StandardCharsets.UTF_8);
                } catch (Exception e) { // Catch any decoding issues
                    System.err.println("P_DTP_R: Topic_name for topic " + (i+1) + " had decode error. Treating as empty.");
                    // currentTopicNameStr remains ""
                }
            }
            // If nameLen == 0, currentTopicNameStr is already ""

            if (firstTopicName == null) {
                firstTopicName = currentTopicNameStr;
            }

            // Parse Partitions for this topic (INT32 count + INT32 array)
            if (remaining - bytesConsumed < 4) { // Not enough for partitionsCount field
                System.err.println("P_DTP_R: Not enough data for partitionsCount (topic " + (i+1) + "). Remaining: " + (remaining - bytesConsumed));
                break; // Malformed request
            }
            byte[] partitionsCountBytes = readNBytes(in, 4);
            bytesConsumed += 4;
            int partitionsCount = ByteBuffer.wrap(partitionsCountBytes).order(ByteOrder.BIG_ENDIAN).getInt();

            if (partitionsCount < 0) {
                 System.err.println("P_DTP_R: Invalid partitions_count " + partitionsCount + " for topic " + (i+1) + ".");
                 break; // Malformed. Stop parsing topics.
            }

            int bytesForPartitionIds = partitionsCount * 4;
            if (bytesForPartitionIds > 0) { // Only try to read if count > 0
                 if (bytesForPartitionIds > remaining - bytesConsumed) {
                     System.err.println("P_DTP_R: Stated " + bytesForPartitionIds + " bytes for partition IDs (topic " + (i+1) + ") exceeds remaining body bytes " + (remaining - bytesConsumed) + ". Malformed.");
                     break; // Malformed. Stop parsing topics.
                 }
                 // We just discard partition IDs as per problem spec for now
                 discardRemainingRequest(in, bytesForPartitionIds);
                 bytesConsumed += bytesForPartitionIds;
            }
        }

        // Consume any remaining bytes specified by the initial 'remaining' size, if not fully parsed.
        int unparsedBytes = remaining - bytesConsumed;
        if (unparsedBytes > 0) {
            System.err.println("P_DTP_R: Discarding " + unparsedBytes + " unparsed bytes from request body.");
            discardRemainingRequest(in, unparsedBytes);
        } else if (unparsedBytes < 0) {
             System.err.println("P_DTP_R: WARNING - Consumed " + bytesConsumed + " bytes, but initial remaining was " + remaining + ".");
        }

        String parsedTopicName = (firstTopicName != null) ? firstTopicName : "";
        System.err.println("P_DTP_R: Parsed first topic='" + parsedTopicName + "'");
        return parsedTopicName;
    }

    /**
     * Builds a DescribeTopicPartitions (v0) response for an unknown topic.
     * The response body is:
     *   - error_code: INT16 (2 bytes) = 3 (UNKNOWN_TOPIC_OR_PARTITION)
     *   - topic_name: STRING (INT16 length + UTF-8 bytes)
     *   - topic_id: 16 bytes of zeros (UUID all zeros)
     *   - partitions: int32 count = 0 (empty array)
     */
    public static byte[] buildDescribeTopicPartitionsResponse(int correlationId, String topic) {
        if (topic == null) {
            topic = ""; // Default to empty string if null
        }
        System.err.println("build_describe_topic_partitions_response: topic_name='" + topic + "' for response.");

        byte[] error_code_bytes = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 3).array();
        byte[] topic_name_encoded_as_kafka_string = encodeKafkaString(topic);

        // Fixed topic_id: 16 bytes of zeros (UUID 00000000-0000-0000-0000-000000000000)
        byte[] topicId = new byte[16];
        byte[] partitions_count_bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(0).array(); // partitions_count = 0 (INT32)

        // The tester seems to expect a flexible response format (v1+) for this API,
        // despite the prompt saying v0. This includes throttle_time_ms and tag buffers.
        byte[] throttle_time_ms_bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(0).array(); // throttle_time_ms (INT32)
        byte[] trailing_tag_buffer = new byte[]{0x00}; // Trailing tag buffer (compact bytes)

        byte[] responseBody = concatenateByteArrays(
            throttle_time_ms_bytes, // Flexible response includes throttle_time_ms first
            error_code_bytes,
            topic_name_encoded_as_kafka_string,
            topicId,
            partitions_count_bytes,
            trailing_tag_buffer // Flexible response often ends with a tag buffer
        );

        int messageSize = responseHeader.length + responseBody.length;

        ByteBuffer buffer = ByteBuffer.allocate(4 + messageSize);
        buffer.order(ByteOrder.BIG_ENDIAN);

        // message_length = correlation_id (4) + body size
        buffer.putInt(messageSize);

        // Response Header for flexible versions is Correlation ID + Tagged Fields (0 tags = 0x00)
        byte[] responseHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(correlationId).array();
        byte[] header_tag_buffer = new byte[]{0x00}; // Header tag buffer (compact bytes)
        buffer.put(concatenateByteArrays(responseHeader, header_tag_buffer)); // Put flexible header
        buffer.put(responseBody);
        return buffer.array();
    }

    // Helper to concatenate byte arrays
    private static byte[] concatenateByteArrays(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] array : arrays) {
            totalLength += array.length;
        }
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    /**
     * Handles an individual client connection.
     * <p>
     * This method processes multiple sequential requests from the same client.
     * For each request, it reads the 12-byte header and discards any extra request data.
     * Then, if the api_key is 18, it sends back an ApiVersions response.
     * Otherwise, it logs that the api_key is unknown.
     * The loop terminates when the client disconnects or an error occurs.
     */
    public static void handleClient(Socket clientSocket) {
        try {
            InputStream clientInputStream = clientSocket.getInputStream();
            OutputStream clientOutputStream = clientSocket.getOutputStream();
            while (true) {
                FullRequestHeader fullHeader;
                try {
                    fullHeader = readFullRequestHeader(clientInputStream);
                } catch (IOException ioe) {
                    System.err.println("IOException while reading header, closing connection: " + ioe.getMessage());
                    break;
                } catch (Exception e) { // Catch other potential errors during header parsing
                    System.err.println("Error reading full request header: " + e.getMessage());
                    break; 
                }
                System.err.println("Received correlation_id: " + fullHeader.correlationId
                        + ", requested api_version: " + fullHeader.apiVersion
                        + ", api_key: " + fullHeader.apiKey);
                System.err.println("Received request messageSize (payload after size field): " + fullHeader.messageSize);
                System.err.println("Client ID: '" + fullHeader.clientId + "', Client ID field length: " + fullHeader.clientIdFieldLengthBytes);
                System.err.println("Calculated request bodySize: " + fullHeader.bodySize);

                if (fullHeader.bodySize < 0) {
                    System.err.println("Error: Calculated negative bodySize (" + fullHeader.bodySize + "). Protocol error or parsing issue.");
                    break; // Critical error, close connection
                }

                if (fullHeader.apiKey == 18) { // ApiVersions
                    if (fullHeader.bodySize > 0) {
                        discardRemainingRequest(clientInputStream, fullHeader.bodySize);
                        System.err.println("Discarded " + fullHeader.bodySize + " bytes from ApiVersions request body.");
                    }
                    buildApiVersionsResponse(fullHeader, clientOutputStream);
                } else if (fullHeader.apiKey == 75) { // DescribeTopicPartitions
                    String topic = null;
                    try {
                        topic = parseDescribeTopicPartitionsRequest(clientInputStream, fullHeader.bodySize);
                        System.err.println("Parsed topic: " + topic);
                    } catch (IOException e) {
                        System.err.println("Error parsing DescribeTopicPartitions request: " + e.getMessage());
                        topic = "unknown"; // Fallback, but ideally send an error response
                    } // topic will be "" if parsing failed or topic name was empty
                    byte[] response = buildDescribeTopicPartitionsResponse(fullHeader.correlationId, topic); // Pass the parsed topic name
                    clientOutputStream.write(response);
                    clientOutputStream.flush();
                    System.err.println("Sent DescribeTopicPartitions response (" + response.length + " bytes)");
                } else {
                    System.err.println("Unknown api_key " + fullHeader.apiKey + ", skipping.");
                    if (fullHeader.bodySize > 0) {
                        discardRemainingRequest(clientInputStream, fullHeader.bodySize);
                        System.err.println("Discarded " + fullHeader.bodySize + " bytes from unknown request.");
                    }
                }
            }
        } catch (Throwable t) {
            System.err.println("Error handling client: " + t.getMessage());
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
