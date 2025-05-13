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

    // Helper class for readKafkaString results
    public static class KafkaStringReadResult {
        public final String value;
        public final int bytesRead; // Total bytes read from stream for this string field

        public KafkaStringReadResult(String value, int bytesRead) {
            this.value = value;
            this.bytesRead = bytesRead;
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
        System.err.println("readNBytes: Requested=" + n + ", Read=" + totalRead + ", Data=" + bytesToHex(data));
        return data;
    }

    // Helper for logging byte arrays as hex
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static KafkaStringReadResult readKafkaString(InputStream in) throws IOException {
        byte[] lengthBytes = readNBytes(in, 2); // readNBytes will print its own log
        short length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.BIG_ENDIAN).getShort();
        System.err.println("readKafkaString: Length bytes=" + bytesToHex(lengthBytes) + ", Length=" + length);

        if (length == -1) { // Nullable string is null
            return new KafkaStringReadResult(null, 2);
        }
        if (length < 0) { // Invalid length
            System.err.println("readKafkaString: Invalid negative length " + length + " (and not -1 for null). Treating as empty string.");
            return new KafkaStringReadResult("", 2);
        }

        byte[] stringPayloadBytes = readNBytes(in, length); // readNBytes will print its own log
        System.err.println("readKafkaString: Read " + length + " bytes for string payload, Data=" + bytesToHex(stringPayloadBytes));
        return new KafkaStringReadResult(new String(stringPayloadBytes, StandardCharsets.UTF_8), 2 + length);
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

        // Read ClientId using the new helper, mimicking Python's read_string call
        KafkaStringReadResult clientIdResult = readKafkaString(in);
        frh.clientId = clientIdResult.value;
        frh.clientIdFieldLengthBytes = clientIdResult.bytesRead;

        int commonHeaderSize = 8; // apiKey (2) + apiVersion (2) + correlationId (4)
        int headerSizeForBodyCalc = commonHeaderSize + frh.clientIdFieldLengthBytes; // Mimics python's header_size calculation
        frh.bodySize = frh.messageSize - headerSizeForBodyCalc;

        // Mimic Python's print statement at the end of read_request_header
        System.err.println("readFullRequestHeader: TotalSize=" + frh.messageSize +
                           ", HeaderSize=" + headerSizeForBodyCalc +
                           ", BodySize=" + frh.bodySize);
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
            int read = in.read(buffer, 0, buffer.length); // Read up to the buffer's capacity (which is the chunk_size)
            if (read == -1) {
                throw new IOException("Unexpected end of stream while discarding " + remaining + " bytes");
            }
            remaining -= read;
        }
    }

    /**
     * Builds an API Versions response, mimicking the Python build_api_versions_response.
     * Handles both success and error (unsupported version) cases.
     * The response includes a flexible header (correlation_id + tag_buffer).
     * <p>
     * Success response body (22 bytes):
     * <ul>
     *   <li>error_code: INT16 (2 bytes, value 0)</li>
     *   <li>api_keys: compact array length: 1 byte (value 2, meaning one element + 1)</li>
     *   <li>An ApiVersion entry (7 bytes) containing:
     *        api_key: INT16 (value 18),
     *        min_version: INT16 (value 0),
     *        max_version: INT16 (value 4),
     *        entry TAG_BUFFER: 1 byte (0x00)
     *   </li>     
     *   <li>A DescribeTopicPartitions entry (7 bytes) containing:
     *        api_key: INT16 (value 75),
     *        min_version: INT16 (value 0),
     *        max_version: INT16 (value 0),
     *        entry TAG_BUFFER: 1 byte (0x00)
     *   </li>     
     *   <li>throttle_time_ms: INT32 (value 0)</li>
     *   <li>overall TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * Error response body (8 bytes for unsupported version):
     * <ul>
     *  <li>error_code: INT16 (2 bytes, value 35)</li>
     *  <li>api_keys: compact array length: 1 byte (value 1, meaning zero entries + 1)</li>
     *  <li>throttle_time_ms: INT32 (value 0)</li>
     *  <li>overall TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * @param correlationId the correlation id from the request.
     * @param requestedApiVersion the api_version from the client's request.
     * @return the complete ApiVersions response bytes.
     */
    public static byte[] buildApiVersionsInternalResponse(int correlationId, short requestedApiVersion) {
        byte[] bodyBytes;
        short errorCode;

        if (requestedApiVersion < 0 || requestedApiVersion > 4) {
            errorCode = 35; // UNSUPPORTED_VERSION
            ByteBuffer errorBodyBuffer = ByteBuffer.allocate(8);
            errorBodyBuffer.order(ByteOrder.BIG_ENDIAN);
            errorBodyBuffer.putShort(errorCode);        // error_code
            errorBodyBuffer.put((byte) 1);              // compact array length (0 entries + 1)
            errorBodyBuffer.putInt(0);                  // throttle_time_ms
            errorBodyBuffer.put((byte) 0);              // overall TAG_BUFFER
            bodyBytes = errorBodyBuffer.array();
        } else {
            errorCode = 0; // Success
            ByteBuffer successBodyBuffer = ByteBuffer.allocate(22);
            successBodyBuffer.order(ByteOrder.BIG_ENDIAN);
            successBodyBuffer.putShort(errorCode);      // error_code = 0
            successBodyBuffer.put((byte) 3);            // compact array length (2 entries + 1)
            
            // Entry 1: ApiVersions (api_key 18)
            successBodyBuffer.putShort((short) 18);     // api_key
            successBodyBuffer.putShort((short) 0);      // min_version
            successBodyBuffer.putShort((short) 4);      // max_version
            successBodyBuffer.put((byte) 0);            // entry TAG_BUFFER
            
            // Entry 2: DescribeTopicPartitions (api_key 75)
            successBodyBuffer.putShort((short) 75);     // api_key
            successBodyBuffer.putShort((short) 0);      // min_version
            successBodyBuffer.putShort((short) 0);      // max_version
            successBodyBuffer.put((byte) 0);            // entry TAG_BUFFER
            
            successBodyBuffer.putInt(0);                // throttle_time_ms
            successBodyBuffer.put((byte) 0);            // overall TAG_BUFFER
            bodyBytes = successBodyBuffer.array();
        }

        // Header part: correlation_id (4 bytes) + tag_buffer (1 byte)
        byte[] headerBytes = ByteBuffer.allocate(5)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(correlationId)
                .put((byte) 0) // Header tag buffer
                .array();

        int messageSizeField = headerBytes.length + bodyBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(4 + messageSizeField);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(messageSizeField); // Total length of (headerBytes + bodyBytes)
        buffer.put(headerBytes);
        buffer.put(bodyBytes);
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
    
        if (topicsCount < 0) {
            System.err.println("P_DTP_R: Invalid topics_count " + topicsCount + ".");
            if (remaining - bytesConsumed > 0) discardRemainingRequest(in, remaining - bytesConsumed);
            return "";
        }
    
        System.err.println("P_DTP_R: Expecting " + topicsCount + " topics.");
    
        for (int i = 0; i < topicsCount; i++) {
            // Parse Topic Name (STRING)
            if (remaining - bytesConsumed < 2) {
                System.err.println("P_DTP_R: Not enough data for topic_name length (topic " + (i+1) + "). Remaining: " + (remaining - bytesConsumed));
                break;
            }
            byte[] nameLenBytes = readNBytes(in, 2);
            bytesConsumed += 2;
            short nameLen = ByteBuffer.wrap(nameLenBytes).order(ByteOrder.BIG_ENDIAN).getShort();
    
            String currentTopicNameStr = "";
            if (nameLen < 0) {
                System.err.println("P_DTP_R: Invalid topic_name length " + nameLen + " for topic " + (i+1) + ". STRING requires >= 0.");
            } else if (nameLen > 0) {
                if (nameLen > remaining - bytesConsumed) {
                    System.err.println("P_DTP_R: Stated topic_name length " + nameLen + " for topic " + (i+1) + " exceeds remaining body bytes " + (remaining - bytesConsumed) + ". Malformed.");
                    if (firstTopicName == null) firstTopicName = "";
                    break;
                }
                byte[] namePayloadBytes = readNBytes(in, nameLen);
                bytesConsumed += nameLen;
                try {
                    currentTopicNameStr = new String(namePayloadBytes, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    System.err.println("P_DTP_R: Topic_name for topic " + (i+1) + " had decode error. Treating as empty.");
                }
            }
    
            if (firstTopicName == null) {
                firstTopicName = currentTopicNameStr;
            }
    
            // Parse Partitions for this topic
            if (remaining - bytesConsumed < 4) {
                System.err.println("P_DTP_R: Not enough data for partitionsCount (topic " + (i+1) + "). Remaining: " + (remaining - bytesConsumed));
                break;
            }
            byte[] partitionsCountBytes = readNBytes(in, 4);
            bytesConsumed += 4;
            int partitionsCount = ByteBuffer.wrap(partitionsCountBytes).order(ByteOrder.BIG_ENDIAN).getInt();
    
            if (partitionsCount < 0) {
                System.err.println("P_DTP_R: Invalid partitions_count " + partitionsCount + " for topic " + (i+1) + ".");
                break;
            }
    
            int bytesForPartitionIds = partitionsCount * 4;
            if (bytesForPartitionIds > 0) {
                if (bytesForPartitionIds > remaining - bytesConsumed) {
                    System.err.println("P_DTP_R: Stated " + bytesForPartitionIds + " bytes for partition IDs (topic " + (i+1) + ") exceeds remaining body bytes " + (remaining - bytesConsumed) + ". Malformed.");
                    break;
                }
                discardRemainingRequest(in, bytesForPartitionIds);
                bytesConsumed += bytesForPartitionIds;
            }
        }
    
        // Consume any remaining bytes
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
    public static byte[] buildDescribeTopicPartitionsResponse(int correlationId, String topic) throws IOException {
        if (topic == null) {
            topic = ""; // Default to empty string if null
        }
        System.err.println("build_describe_topic_partitions_response: topic_name='" + topic + "' for response.");
    
        byte[] errorCodeBytes = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 3).array(); // UNKNOWN_TOPIC_OR_PARTITION
        byte[] topicNameEncoded = encodeKafkaString(topic); // Properly encode as Kafka STRING
        byte[] topicId = new byte[16]; // 16 zeros (nil UUID)
        byte[] partitionsCountBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(0).array(); // partitions_count = 0
    
        byte[] responseBody = concatenateByteArrays(
            errorCodeBytes,
            topicNameEncoded,
            topicId,
            partitionsCountBytes
        );
    
        byte[] responseHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(correlationId).array();
        int messageSize = responseHeader.length + responseBody.length;
    
        ByteBuffer buffer = ByteBuffer.allocate(4 + messageSize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(messageSize); // message_length
        buffer.put(responseHeader);
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
                byte[] response = null;
                FullRequestHeader fullHeader;
                try {
                    fullHeader = readFullRequestHeader(clientInputStream);
                } catch (IOException ioe) {
                    System.err.println("IOException while reading header, closing connection: " + ioe.getMessage());
                    break;
                } catch (Exception e) {
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
                    break;
                }
    
                if (fullHeader.apiKey == 18) {
                    if (fullHeader.bodySize > 0) {
                        discardRemainingRequest(clientInputStream, fullHeader.bodySize);
                        System.err.println("Discarded " + fullHeader.bodySize + " bytes from ApiVersions request body.");
                    }
                    // Call the consolidated method that handles both success and error based on version
                    response = buildApiVersionsInternalResponse(fullHeader.correlationId, fullHeader.apiVersion);
                    System.err.println("Prepared ApiVersions response (" + (response != null ? response.length : 0) + " bytes)");
                } else if (fullHeader.apiKey == 75) {
                    if (fullHeader.apiVersion == 0) {
                        String topicName;
                        try {
                            topicName = parseDescribeTopicPartitionsRequest(clientInputStream, fullHeader.bodySize);
                            System.err.println("Parsed DescribeTopicPartitions v0 request for topic: '" + topicName + "'");
                        } catch (IOException e) {
                            System.err.println("Error parsing DescribeTopicPartitions v0 request: " + e.getMessage());
                            topicName = ""; // Fallback on parsing error
                        }
                        response = buildDescribeTopicPartitionsResponse(fullHeader.correlationId, topicName);
                        System.err.println("Prepared DescribeTopicPartitions v0 (Unknown Topic) response (" + (response != null ? response.length : 0) + " bytes)");
                    } else {
                        System.err.println("Unsupported DescribeTopicPartitions version: " + fullHeader.apiVersion + ". Discarding body.");
                        if (fullHeader.bodySize > 0) {
                            discardRemainingRequest(clientInputStream, fullHeader.bodySize);
                        }
                        // No response for unsupported version, response remains null
                    }
                } else {
                    System.err.println("Unknown api_key " + fullHeader.apiKey + ", skipping.");
                    if (fullHeader.bodySize > 0) {
                        discardRemainingRequest(clientInputStream, fullHeader.bodySize);
                        System.err.println("Discarded " + fullHeader.bodySize + " bytes from unknown request.");
                    }
                }

                if (response != null) {
                    clientOutputStream.write(response);
                    clientOutputStream.flush();
                }
            }
        } catch (Throwable t) {
            System.err.println("Error handling client: " + t.getMessage());
        } finally {
            try {
                clientSocket.shutdownOutput();
            } catch (IOException e) {}
            try {
                clientSocket.close();
            } catch (IOException e) {}
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
