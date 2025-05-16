import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

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
    private static ClusterMetadataReader metadataReader;

    /**
     * Encodes a Java String into Kafka's COMPACT_STRING format (1-byte length + UTF-8 bytes).
     * A null string is encoded as INT16 -1.
     * @param s the string to encode.
     * @return the encoded bytes.
     */
    public static byte[] encodeKafkaString(String s) {
        if (s == null) {
            return new byte[]{(byte) 1}; // Length=1 (empty)
        }
        byte[] utf8Bytes = s.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[1 + utf8Bytes.length];
        result[0] = (byte) (utf8Bytes.length + 1);
        System.arraycopy(utf8Bytes, 0, result, 1, utf8Bytes.length);
        return result;
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
     * Handles success (v0–3, v4) and error (unsupported version) cases.
     * The response includes a flexible header (correlation_id + tag_buffer).
     * <p>
     * Success response body for api_version 0–3 (22 bytes):
     * <ul>
     *   <li>error_code: INT16 (2 bytes, value 0)</li>
     *   <li>api_keys: compact array length: 1 byte (value 3, meaning two elements + 1)</li>
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
     * Success response body for api_version 4 (8 bytes):
     * <ul>
     *   <li>error_code: INT16 (2 bytes, value 0)</li>
     *   <li>api_keys: compact array length: 1 byte (value 1, meaning zero entries + 1)</li>
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
    public static byte[] buildApiVersionsInternalResponse(int correlationId, short apiVersion) {
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        
        try {
            // Check if the requested API version is supported
            boolean isSupported = apiVersion >= 0 && apiVersion <= 4;
            short errorCode = isSupported ? (short) 0 : (short) 35;
            
            // Calculate total size (will be filled in later)
            response.write(new byte[4]);
            
            // Response header: correlation ID
            response.write(ByteBuffer.allocate(4).putInt(correlationId).array());
            
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

    /**
     * Result class for parsing DescribeTopicPartitions request, mimicking Python's parse_describe_topic_partitions.
     */
    public static class DescribeTopicPartitionsParseResult {
        public final String topicName;
        public final byte arrayLength;
        public final byte topicNameLength;
        public final byte cursor;

        public DescribeTopicPartitionsParseResult(String topicName, byte arrayLength, byte topicNameLength, byte cursor) {
            this.topicName = topicName;
            this.arrayLength = arrayLength;
            this.topicNameLength = topicNameLength;
            this.cursor = cursor;
        }
    }

    /**
     * Parses a DescribeTopicPartitions (v0) request body, mimicking Python's parse_describe_topic_partitions.
     * The request body format (compact, as per Python):
     *  - topics: compact array of topics:
     *       int8 topicsCount (1 byte)
     *       For each topic:
     *          string topic_name (1-byte length + UTF-8 bytes)
     *          int32 partitionsCount
     *          For each partition: int32 partition id
     * Returns the first topic name, array_length, topic_name_length, and cursor.
     */
    public static DescribeTopicPartitionsParseResult parseDescribeTopicPartitionsRequest(
            InputStream in, 
            int remaining, 
            int clientIdLen
        ) throws IOException {
        int bytesConsumed = 0;
        byte arrayLength = 0;
        byte topicNameLength = 0;
        byte cursor = 0;
        String topicName = "";

        // Read array_length (1 byte, compact)
        if (remaining < 1) {
            System.err.println("P_DTP_R: Body too small for array_length (" + remaining + " bytes).");
            return new DescribeTopicPartitionsParseResult("", (byte) 0, (byte) 0, (byte) 0);
        }
        byte[] arrayLengthBytes = readNBytes(in, 1);
        arrayLength = arrayLengthBytes[0];
        bytesConsumed += 1;
        System.err.println("P_DTP_R: array_length=" + (arrayLength & 0xFF));

        // Read topic_name_length (1 byte, compact)
        if (remaining - bytesConsumed < 1) {
            System.err.println("P_DTP_R: Not enough data for topic_name_length. Remaining: " + (remaining - bytesConsumed));
            return new DescribeTopicPartitionsParseResult("", arrayLength, (byte) 0, (byte) 0);
        }
        byte[] topicNameLengthBytes = readNBytes(in, 1);
        topicNameLength = topicNameLengthBytes[0];
        bytesConsumed += 1;
        System.err.println("P_DTP_R: topic_name_length=" + (topicNameLength & 0xFF));

        // Read topic_name (topicNameLength - 1 bytes, to match Python bug)
        int topicBytesToRead = (topicNameLength & 0xFF) - 1;
        if (topicBytesToRead > 0) {
            if (topicBytesToRead > remaining - bytesConsumed) {
                System.err.println("P_DTP_R: Stated topic_name length " + topicBytesToRead + " exceeds remaining bytes " + (remaining - bytesConsumed));
                return new DescribeTopicPartitionsParseResult("", arrayLength, topicNameLength, (byte) 0);
            }
            byte[] topicNameBytes = readNBytes(in, topicBytesToRead);
            bytesConsumed += topicBytesToRead;
            try {
                topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
            } catch (Exception e) {
                System.err.println("P_DTP_R: Topic_name decode error: " + e.getMessage());
                topicName = "";
            }
        }
        System.err.println("P_DTP_R: topic_name='" + topicName + "'");

        // Skip to cursor (topic_name_starter + topic_name_length + 4)
        int cursorOffset = 14 + clientIdLen + 1 + 2 + (topicNameLength & 0xFF) + 4;
        int bytesToSkip = cursorOffset - (14 + clientIdLen + 1 + bytesConsumed);
        if (bytesToSkip > 0) {
            if (bytesToSkip > remaining - bytesConsumed) {
                System.err.println("P_DTP_R: Cannot skip " + bytesToSkip + " bytes to cursor. Remaining: " + (remaining - bytesConsumed));
                return new DescribeTopicPartitionsParseResult(topicName, arrayLength, topicNameLength, (byte) 0);
            }
            discardRemainingRequest(in, bytesToSkip);
            bytesConsumed += bytesToSkip;
        }

        // Read cursor (1 byte)
        if (remaining - bytesConsumed >= 1) {
            byte[] cursorBytes = readNBytes(in, 1);
            cursor = cursorBytes[0];
            bytesConsumed += 1;
            System.err.println("P_DTP_R: cursor=" + (cursor & 0xFF));
        } else {
            System.err.println("P_DTP_R: Not enough data for cursor. Remaining: " + (remaining - bytesConsumed));
        }

        // Discard remaining bytes
        int unparsedBytes = remaining - bytesConsumed;
        if (unparsedBytes > 0) {
            System.err.println("P_DTP_R: Discarding " + unparsedBytes + " unparsed bytes.");
            discardRemainingRequest(in, unparsedBytes);
        }

        return new DescribeTopicPartitionsParseResult(topicName, arrayLength, topicNameLength, cursor);
    }

    /**
     * Builds a DescribeTopicPartitions (v0) response, supporting both known and unknown topics.
     * Uses ClusterMetadataReader to check topic existence and retrieve metadata.
     * Response format (flexible, mimics v1+):
     * - throttle_time_ms: INT32 (0)
     * - topics: COMPACT_ARRAY
     *   - array_length: 1 byte
     *   - error_code: INT16 (0 for known, 3 for unknown)
     *   - topic_name_length: 1 byte
     *   - topic_name: UTF-8 bytes
     *   - topic_id: UUID (16 bytes, from metadata or zeros)
     *   - is_internal: BOOLEAN (0)
     *   - partitions: COMPACT_ARRAY
     *     - array_length: 1 byte (2 for 1 partition, 1 for empty)
     *     - partition_index: INT32
     *     - error_code: INT16 (0)
     *     - leader_id: INT32
     *     - leader_epoch: INT32
     *     - replica_nodes: COMPACT_ARRAY of INT32
     *     - isr_nodes: COMPACT_ARRAY of INT32
     *     - offline_replicas: COMPACT_ARRAY of INT32
     *     - tagged_fields: 1 byte (0)
     *   - topic_authorized_operations: INT32 (0x00000df8)
     *   - tagged_fields: 1 byte (0)
     * - cursor: 1 byte
     * - tagged_fields: 1 byte (0)
     */
    public static byte[] buildDescribeTopicPartitionsResponse(
            int correlationId, String topic, byte arrayLength, byte topicNameLength, byte cursor) {
        if (topic == null) {
            topic = "";
        }
        System.err.println("build_describe_topic_partitions_response: topic_name='" + topic +
                           "', array_length=" + (arrayLength & 0xFF) +
                           ", topic_name_length=" + (topicNameLength & 0xFF) +
                           ", cursor=" + (cursor & 0xFF));

        ByteBuffer bodyBuffer = ByteBuffer.allocate(1024); // Allocate enough for known/unknown cases
        bodyBuffer.order(ByteOrder.BIG_ENDIAN);

        // Common fields
        bodyBuffer.putInt(0); // throttle_time_ms
        bodyBuffer.put(arrayLength); // topics_array_length

        // Topic entry
        TopicMetadata metadata = metadataReader.getTopicMetadata(topic);
        boolean isKnown = metadata != null && metadataReader.topicExists(topic);
        short errorCode = isKnown ? (short) 0 : (short) 3;

        bodyBuffer.putShort(errorCode);
        bodyBuffer.put(encodeKafkaString(topic)); // topic_name (COMPACT_STRING)
        
        // topic_id
        if (isKnown) {
            bodyBuffer.putLong(metadata.topicId.getMostSignificantBits());
            bodyBuffer.putLong(metadata.topicId.getLeastSignificantBits());
        } else {
            bodyBuffer.put(new byte[16]); // Zeroed UUID
        }

        bodyBuffer.put((byte) 0); // is_internal

        // Partitions array
        if (isKnown && !metadata.partitions.isEmpty()) {
            bodyBuffer.put((byte) 2); // partitions_array_length (1 partition + 1)
            PartitionMetadata partition = metadata.partitions.get(0);
            
            bodyBuffer.putInt(partition.partitionIndex); // partition_index
            bodyBuffer.putShort((short) 0); // error_code
            bodyBuffer.putInt(partition.leaderId); // leader_id
            bodyBuffer.putInt(partition.leaderEpoch); // leader_epoch
            
            // replica_nodes (assume [0] per metadata)
            bodyBuffer.put((byte) 2); // length=1+1
            bodyBuffer.putInt(0);
            
            // isr_nodes
            bodyBuffer.put((byte) (partition.isr.size() + 1));
            for (Integer node : partition.isr) {
                bodyBuffer.putInt(node);
            }
            
            // offline_replicas (empty)
            bodyBuffer.put((byte) 1);
            
            bodyBuffer.put((byte) 0); // partition tagged_fields
        } else {
            bodyBuffer.put((byte) 1); // empty partitions array
        }

        bodyBuffer.putInt(0x00000df8); // topic_authorized_operations
        bodyBuffer.put((byte) 0); // topic_tag_buffer
        bodyBuffer.put(cursor); // cursor
        bodyBuffer.put((byte) 0); // response_tag_buffer

        // Trim buffer to actual size
        byte[] responseBody = new byte[bodyBuffer.position()];
        bodyBuffer.flip();
        bodyBuffer.get(responseBody);

        byte[] responseHeader = ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN)
            .putInt(correlationId)
            .put((byte) 0)
            .array();
        int messageSize = responseHeader.length + responseBody.length;

        ByteBuffer buffer = ByteBuffer.allocate(4 + messageSize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(messageSize);
        buffer.put(responseHeader);
        buffer.put(responseBody);
        return buffer.array();
    }

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

    private static class ClientHandler {
        static class ParseResult {
            final short apiKey;
            final short apiVersion;
            final int correlationId;
            final String clientId;
            final int clientIdLen;
            final int bodySize;

            ParseResult(short apiKey, short apiVersion, int correlationId, String clientId, int clientIdLen, int bodySize) {
                this.apiKey = apiKey;
                this.apiVersion = apiVersion;
                this.correlationId = correlationId;
                this.clientId = clientId;
                this.clientIdLen = clientIdLen;
                this.bodySize = bodySize;
            }
        }

        static ParseResult parseRequestHeader(InputStream in) throws IOException {
            FullRequestHeader header = readFullRequestHeader(in);
            System.err.println("parse_request_header: api_key=" + header.apiKey +
                               ", api_version=" + header.apiVersion +
                               ", correlation_id=" + header.correlationId);
            return new ParseResult(header.apiKey, header.apiVersion, header.correlationId,
                                   header.clientId, header.clientIdFieldLengthBytes, header.bodySize);
        }

        static byte parseTaggedField(InputStream in, int remaining) throws IOException {
            if (remaining < 1) {
                System.err.println("parse_tagged_field: Not enough data for tagged field. Remaining: " + remaining);
                return 0;
            }
            byte[] taggedBytes = readNBytes(in, 1);
            System.err.println("parse_tagged_field: tagged=" + (taggedBytes[0] & 0xFF));
            return taggedBytes[0];
        }

        static void handleApiVersionsRequest(OutputStream out, int correlationId, short apiVersion, int bodySize, InputStream in) throws IOException {
            if (bodySize > 0) {
                discardRemainingRequest(in, bodySize);
                System.err.println("Discarded " + bodySize + " bytes from ApiVersions request body.");
            }
            byte[] response = buildApiVersionsInternalResponse(correlationId, apiVersion);
            out.write(response);
            out.flush();
            System.err.println("Sent ApiVersions response (" + response.length + " bytes)");
        }

        static void handleDescribeTopicPartitionsRequest(
            OutputStream out, int correlationId, 
            short apiVersion, int bodySize, 
            InputStream in, int clientIdLen
        ) throws IOException {
            if (apiVersion != 0) {
                System.err.println("Unsupported DescribeTopicPartitions version: " + apiVersion + ". Discarding body.");
                if (bodySize > 0) {
                    discardRemainingRequest(in, bodySize);
                }
                return;
            }
            // Read tagged field after client_id
            int remaining = bodySize;
            byte tagged = parseTaggedField(in, remaining);
            remaining -= 1;
            DescribeTopicPartitionsParseResult parseResult;
            try {
                parseResult = parseDescribeTopicPartitionsRequest(in, remaining, clientIdLen);
                System.err.println("Parsed DescribeTopicPartitions v0 request for topic: '" + parseResult.topicName + "'");
            } catch (IOException e) {
                System.err.println("Error parsing DescribeTopicPartitions v0 request: " + e.getMessage());
                parseResult = new DescribeTopicPartitionsParseResult("", (byte) 0, (byte) 0, (byte) 0);
            }
            byte[] response = buildDescribeTopicPartitionsResponse(
                correlationId, parseResult.topicName, parseResult.arrayLength, parseResult.topicNameLength, parseResult.cursor);
            out.write(response);
            out.flush();
            System.err.println("Sent DescribeTopicPartitions v0 response (" + response.length + " bytes)");
        }

        static void handleUnknownRequest(int apiKey, int bodySize, InputStream in) throws IOException {
            System.err.println("Unknown api_key " + apiKey + ", skipping.");
            if (bodySize > 0) {
                discardRemainingRequest(in, bodySize);
                System.err.println("Discarded " + bodySize + " bytes from unknown request.");
            }
        }
    }

    /**
     * Handles an individual client connection, mimicking Python's handle_client.
     * Processes multiple sequential requests from the same client.
     */
    public static void handleClient(Socket clientSocket) {
        try {
            InputStream in = clientSocket.getInputStream();
            OutputStream out = clientSocket.getOutputStream();
            while (true) {
                ClientHandler.ParseResult header;
                try {
                    header = ClientHandler.parseRequestHeader(in);
                } catch (IOException e) {
                    System.err.println("IOException while reading header, closing connection: " + e.getMessage());
                    break;
                } catch (Exception e) {
                    System.err.println("Error reading full request header: " + e.getMessage());
                    break;
                }
                System.err.println("Received correlation_id: " + header.correlationId +
                                   ", requested api_version: " + header.apiVersion +
                                   ", api_key: " + header.apiKey +
                                   ", client_id: '" + header.clientId + "'" +
                                   ", client_id_length: " + header.clientIdLen +
                                   ", body_size: " + header.bodySize);

                if (header.bodySize < 0) {
                    System.err.println("Error: Calculated negative bodySize (" + header.bodySize + "). Protocol error or parsing issue.");
                    break;
                }
                if (header.apiKey == 18) {
                    ClientHandler.handleApiVersionsRequest(out, header.correlationId, header.apiVersion, header.bodySize, in);
                } else if (header.apiKey == 75) {
                    ClientHandler.handleDescribeTopicPartitionsRequest(out, header.correlationId, header.apiVersion, header.bodySize, in, header.clientIdLen);
                } else {
                    ClientHandler.handleUnknownRequest(header.apiKey, header.bodySize, in);
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
            metadataReader = new ClusterMetadataReader();
            runServer(port);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
